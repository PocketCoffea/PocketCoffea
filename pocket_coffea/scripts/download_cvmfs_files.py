#!/usr/bin/env python3
"""
Utility script to download CVMFS files referenced in PocketCoffea parameters
and store them locally with versioned metadata and hard links for efficiency.

This script:
1. Scans parameter files for CVMFS references (both direct and resolver syntax)
2. Downloads files to versioned directories
3. Uses hard links when files haven't changed between versions
4. Stores comprehensive metadata about each version
"""

import os
import sys
import re
import json
import shutil
import hashlib
import glob
import click
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Set, List, Dict, Any

try:
    import yaml
    from omegaconf import OmegaConf
except ImportError:
    print("Error: Required packages not found. Install with:")
    print("pip install pyyaml omegaconf")
    sys.exit(1)


class CVMFSFileDownloader:
    """Download and manage versioned CVMFS files with hard link deduplication."""
    
    def __init__(self, output_dir: Path, tag: str, dry_run: bool = False, 
                 max_workers: int = 4, verbose: bool = False):
        self.output_dir = output_dir
        self.tag = tag
        self.dry_run = dry_run
        self.max_workers = max_workers
        self.verbose = verbose
        self.metadata = {
            "download_date": datetime.now().isoformat(),
            "tag": self.tag,
            "files": []
        }
        
        if not self.dry_run:
            # Create the versioned output directory
            versioned_dir = self.output_dir / self.tag
            versioned_dir.mkdir(parents=True, exist_ok=True)
    
    def find_cvmfs_files(self, parameter_files: List[Path]) -> Set[str]:
        """Find all CVMFS file references in parameter files."""
        print("Scanning parameter files for CVMFS references...")
        
        # Pattern to match old-style direct CVMFS paths
        direct_cvmfs_pattern = re.compile(r'/cvmfs/[^\s\'\"]+')
        
        # Pattern to match new-style cvmfs resolver syntax: ${cvmfs:relative_path}
        resolver_cvmfs_pattern = re.compile(r'\$\{cvmfs:([^}]+)\}')
        
        cvmfs_files = set()
        
        for file_path in parameter_files:
            if not file_path.exists():
                print(f"Warning: {file_path} not found")
                continue
                
            print(f"Scanning {file_path}...")
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                    
                    # Find old-style direct CVMFS paths
                    direct_matches = direct_cvmfs_pattern.findall(content)
                    for match in direct_matches:
                        # Skip docker images and other non-file references
                        if not any(ext in match for ext in ['.json', '.gz', '.txt', '.root']):
                            continue
                        cvmfs_files.add(match)
                        if self.verbose:
                            print(f"  Found direct path: {match}")
                    
                    # Find new-style resolver paths
                    resolver_matches = resolver_cvmfs_pattern.findall(content)
                    for relative_path in resolver_matches:
                        # Convert to full CVMFS path for downloading
                        if relative_path.startswith('/'):
                            full_path = f"/cvmfs{relative_path}"
                        else:
                            full_path = f"/cvmfs/{relative_path}"
                        # Skip docker images and other non-file references
                        if not any(ext in full_path for ext in ['.json', '.gz', '.txt', '.root']):
                            continue
                        cvmfs_files.add(full_path)
                        if self.verbose:
                            print(f"  Found resolver path: {relative_path} -> {full_path}")
                            
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
        
        print(f"\nFound {len(cvmfs_files)} unique CVMFS files")
        return cvmfs_files
    
    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _find_previous_version_file(self, checksum: str, relative_path: str) -> Optional[Path]:
        """Find a file with the same checksum in previous versions for hard linking."""
        if not self.output_dir.exists():
            return None
        
        # Look through all version directories except current
        for version_dir in self.output_dir.iterdir():
            if not version_dir.is_dir() or version_dir.name == self.tag:
                continue
            
            # Check if the same file exists in this version
            potential_file = version_dir / relative_path
            if potential_file.exists():
                try:
                    if self.calculate_checksum(potential_file) == checksum:
                        return potential_file
                except Exception:
                    continue
        
        return None
    
    def download_file(self, cvmfs_path: str) -> bool:
        """Download a single CVMFS file with versioning and hard link support."""
        if self.verbose:
            print(f"Processing: {cvmfs_path}")
        
        # Create local path structure in versioned directory
        rel_path = cvmfs_path.replace('/cvmfs/', '')
        versioned_dir = self.output_dir / self.tag
        local_path = versioned_dir / rel_path
        
        # File metadata
        file_info = {
            "cvmfs_path": cvmfs_path,
            "local_path": str(local_path),
            "download_date": datetime.now().isoformat(),
            "status": "error",
            "checksum": "",
            "size": 0,
            "changed_in_tag": self.tag
        }
        
        if self.dry_run:
            file_info["status"] = "would_download"
            print(f"  Would download to: {local_path}")
            self.metadata["files"].append(file_info)
            return True
        
        try:
            # Check if file already exists locally
            if local_path.exists():
                file_info["status"] = "already_exists"
                file_info["checksum"] = self.calculate_checksum(local_path)
                file_info["size"] = local_path.stat().st_size
                if self.verbose:
                    print(f"  Already exists: {local_path}")
                self.metadata["files"].append(file_info)
                return True
            
            # Create directory structure
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if CVMFS file exists and get its checksum
            if not Path(cvmfs_path).exists():
                print(f"  Error: CVMFS file not found: {cvmfs_path}")
                file_info["status"] = "cvmfs_not_found"
                self.metadata["files"].append(file_info)
                return False
            
            # Calculate checksum of source file
            source_checksum = self.calculate_checksum(Path(cvmfs_path))
            file_info["checksum"] = source_checksum
            file_info["size"] = Path(cvmfs_path).stat().st_size
            
            # Try to find previous version with same checksum for hard linking
            previous_file = self._find_previous_version_file(source_checksum, rel_path)
            if previous_file:
                # Create hard link instead of copying
                try:
                    os.link(str(previous_file), str(local_path))
                    file_info["status"] = "hardlinked"
                    file_info["hardlinked_from"] = str(previous_file)
                    # Find which tag this file last changed in
                    for version_dir in self.output_dir.iterdir():
                        if version_dir.is_dir() and previous_file.is_relative_to(version_dir):
                            file_info["changed_in_tag"] = version_dir.name
                            break
                    if self.verbose:
                        print(f"  Hard linked from: {previous_file}")
                except OSError as e:
                    # Hard link failed, fall back to copy
                    if self.verbose:
                        print(f"  Hard link failed ({e}), copying instead")
                    shutil.copy2(cvmfs_path, local_path)
                    file_info["status"] = "downloaded"
            else:
                # Copy file (first time or changed)
                shutil.copy2(cvmfs_path, local_path)
                file_info["status"] = "downloaded"
                if self.verbose:
                    print(f"  Downloaded: {cvmfs_path} -> {local_path}")
            
            # Verify checksum
            local_checksum = self.calculate_checksum(local_path)
            if local_checksum != source_checksum:
                print(f"  Error: Checksum mismatch for {cvmfs_path}")
                file_info["status"] = "checksum_error"
                self.metadata["files"].append(file_info)
                return False
            
            self.metadata["files"].append(file_info)
            return True
            
        except Exception as e:
            print(f"  Error downloading {cvmfs_path}: {e}")
            file_info["status"] = "error"
            file_info["error"] = str(e)
            self.metadata["files"].append(file_info)
            return False
    
    def download_files(self, parameter_files: List[Path]) -> tuple[int, int]:
        """Main entry point to download all files."""
        # Find all CVMFS files
        cvmfs_files = self.find_cvmfs_files(parameter_files)
        
        if not cvmfs_files:
            print("No CVMFS files found.")
            return 0, 0
        
        print(f"Starting download of {len(cvmfs_files)} files...")
        
        # Download files
        success_count = 0
        error_count = 0
        
        for cvmfs_path in sorted(cvmfs_files):
            success = self.download_file(cvmfs_path)
            if success:
                success_count += 1
            else:
                error_count += 1
        
        print(f"\nDownload Summary:")
        print(f"  Total: {len(cvmfs_files)}")
        print(f"  Success: {success_count}")
        print(f"  Errors: {error_count}")
        
        # Save metadata
        self.save_metadata()
        
        return success_count, error_count
    
    def save_metadata(self):
        """Save metadata about downloaded files in the versioned directory."""
        if self.dry_run:
            versioned_dir = self.output_dir / self.tag
            print(f"\nWould save metadata to: {versioned_dir / 'metadata.json'}")
            return
            
        # Create versioned directory if it doesn't exist
        versioned_dir = self.output_dir / self.tag
        versioned_dir.mkdir(parents=True, exist_ok=True)
        
        metadata_file = versioned_dir / "metadata.json"
        
        # Add summary statistics
        self.metadata["summary"] = {
            "total_files": len(self.metadata["files"]),
            "downloaded": len([f for f in self.metadata["files"] if f["status"] == "downloaded"]),
            "hardlinked": len([f for f in self.metadata["files"] if f["status"] == "hardlinked"]),
            "already_existed": len([f for f in self.metadata["files"] if f["status"] == "already_exists"]),
            "errors": len([f for f in self.metadata["files"] if f["status"] == "error"])
        }
        
        # Add version comparison information
        self.metadata["version_info"] = self._generate_version_comparison()
        
        with open(metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2, sort_keys=True)
        
        print(f"\nMetadata saved to: {metadata_file}")
        
        # Also create/update a global index of all versions
        self._update_global_version_index()
    
    def _generate_version_comparison(self) -> Dict[str, Any]:
        """Generate comparison information with previous versions."""
        version_info = {
            "current_tag": self.tag,
            "previous_versions": [],
            "file_changes": {
                "new_files": [],
                "changed_files": [],
                "unchanged_files": []
            }
        }
        
        # Find previous versions
        try:
            if self.output_dir.exists():
                for item in self.output_dir.iterdir():
                    if item.is_dir() and item.name != self.tag:
                        version_info["previous_versions"].append(item.name)
        except OSError:
            pass
        
        version_info["previous_versions"].sort()
        
        # Categorize files by change status
        for file_info in self.metadata["files"]:
            if file_info["status"] == "error":
                continue
                
            if file_info["status"] == "downloaded":
                if file_info["changed_in_tag"] == self.tag:
                    version_info["file_changes"]["new_files"].append({
                        "path": file_info["cvmfs_path"],
                        "checksum": file_info["checksum"]
                    })
                else:
                    version_info["file_changes"]["changed_files"].append({
                        "path": file_info["cvmfs_path"],
                        "checksum": file_info["checksum"],
                        "last_changed": file_info["changed_in_tag"]
                    })
            elif file_info["status"] in ["hardlinked", "already_exists"]:
                version_info["file_changes"]["unchanged_files"].append({
                    "path": file_info["cvmfs_path"],
                    "checksum": file_info["checksum"],
                    "last_changed": file_info["changed_in_tag"]
                })
        
        return version_info
    
    def _update_global_version_index(self):
        """Create or update a global index of all versions."""
        index_file = self.output_dir / "versions_index.json"
        
        # Load existing index or create new one
        if index_file.exists():
            try:
                with open(index_file, 'r') as f:
                    index = json.load(f)
                if not isinstance(index, dict) or "versions" not in index:
                    index = {"versions": {}, "last_updated": "", "latest_tag": ""}
            except Exception:
                index = {"versions": {}, "last_updated": "", "latest_tag": ""}
        else:
            index = {"versions": {}, "last_updated": "", "latest_tag": ""}
        
        # Add current version to index
        index["versions"][self.tag] = {
            "created": datetime.now().isoformat(),
            "total_files": len(self.metadata["files"]),
            "downloaded": len([f for f in self.metadata["files"] if f["status"] == "downloaded"]),
            "hardlinked": len([f for f in self.metadata["files"] if f["status"] == "hardlinked"]),
            "errors": len([f for f in self.metadata["files"] if f["status"] == "error"])
        }
        
        index["last_updated"] = datetime.now().isoformat()
        index["latest_tag"] = self.tag
        
        # Save updated index
        with open(index_file, 'w') as f:
            json.dump(index, f, indent=2, sort_keys=True)
        
        print(f"Updated global version index: {index_file}")


def list_versions(output_dir: Path):
    """List available versions in the output directory."""
    print(f"Checking versions in: {output_dir}")
    print()
    
    if not output_dir.exists():
        print("Output directory does not exist yet.")
        return
    
    # Check for versions index
    index_file = output_dir / "versions_index.json"
    if index_file.exists():
        try:
            with open(index_file, 'r') as f:
                index = json.load(f)
            
            versions = index.get("versions", {})
            if not versions:
                print("No versions found.")
                return
            
            print("Available versions:")
            print("-" * 60)
            for tag, info in sorted(versions.items()):
                print(f"Tag: {tag}")
                print(f"  Created: {info.get('created', 'Unknown')}")
                print(f"  Files: {info.get('total_files', 0)} total, "
                      f"{info.get('downloaded', 0)} downloaded, "
                      f"{info.get('hardlinked', 0)} hard-linked")
                if info.get('errors', 0) > 0:
                    print(f"  Errors: {info.get('errors', 0)}")
                print()
                
            latest = index.get("latest_tag")
            if latest:
                print(f"Latest version: {latest}")
        
        except Exception as e:
            print(f"Error reading versions index: {e}")
            print("Falling back to directory scan...")
    
    # Fallback: scan directories
    version_dirs = []
    try:
        for item in output_dir.iterdir():
            if item.is_dir():
                metadata_file = item / "metadata.json"
                if metadata_file.exists():
                    version_dirs.append(item.name)
    except OSError:
        pass
    
    if version_dirs:
        print("Found version directories:")
        for version in sorted(version_dirs):
            print(f"  {version}")
    else:
        print("No version directories found.")


@click.command()
@click.option(
    "--parameter-files", "-p",
    multiple=True,
    help="Parameter YAML files to scan for CVMFS paths. If not specified, scans all files in params/"
)
@click.option(
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    default=Path("current_cvmfs_files"),
    help="Output directory for downloaded files (default: current_cvmfs_files)"
)
@click.option(
    "--tag", "-t",
    type=str,
    required=True,
    help="Version tag for this download session (creates subdirectory)"
)
@click.option(
    "--dry-run", "-n",
    is_flag=True,
    help="Show what would be downloaded without actually downloading"
)
@click.option(
    "--list-versions", "-l",
    is_flag=True,
    help="List available versions in the output directory"
)
@click.option(
    "--workers", "-w",
    type=int,
    default=4,
    help="Number of concurrent download workers (default: 4)"
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output"
)
def download_cvmfs_files(parameter_files, output_dir, tag, dry_run, list_versions, workers, verbose):
    """Download CVMFS files referenced in PocketCoffea parameter files.
    
    This tool scans parameter files for CVMFS references (both direct and resolver syntax),
    downloads them to versioned directories, and uses hard links for efficiency when files
    haven't changed between versions.
    
    Examples:
    
      # Download with tag v1.0 to current_cvmfs_files/v1.0/
      pocket-coffea download-cvmfs-files --tag v1.0 --parameter-files params/object_preselection.yaml

      # Dry run to see what would be downloaded
      pocket-coffea download-cvmfs-files --tag v1.0 --dry-run

      # Download all parameter files in a directory
      pocket-coffea download-cvmfs-files --tag v2.0 --parameter-files params/*.yaml

      # Use custom output directory
      pocket-coffea download-cvmfs-files --tag v1.0 --output-dir /path/to/storage --parameter-files params/*.yaml

      # List available versions
      pocket-coffea download-cvmfs-files --list-versions
    
    The tool creates a versioned directory structure like:
      output_dir/
      ├── tag1/
      │   ├── file1.txt
      │   ├── file2.txt
      │   └── metadata.json
      ├── tag2/
      │   ├── file1.txt (hard link if unchanged)
      │   ├── file3.txt (new file)
      │   └── metadata.json
      └── versions_index.json
    """
    
    # Handle list-versions command
    if list_versions:
        list_versions(output_dir)
        return
    
    # Determine parameter files to scan
    if not parameter_files:
        # Default: scan all YAML files in params/
        params_dir = Path("params")
        if params_dir.exists():
            parameter_files_list = list(params_dir.glob("*.yaml")) + list(params_dir.glob("*.yml"))
            if not parameter_files_list:
                print(f"No YAML files found in {params_dir}")
                sys.exit(1)
        else:
            print(f"Default params directory not found: {params_dir}")
            print("Please specify parameter files with --parameter-files")
            sys.exit(1)
    else:
        # Expand globs in parameter files
        parameter_files_list = []
        for pattern in parameter_files:
            matches = glob.glob(pattern)
            if matches:
                parameter_files_list.extend(Path(f) for f in matches)
            else:
                parameter_files_list.append(Path(pattern))
        
        # Check if files exist
        missing_files = [f for f in parameter_files_list if not f.exists()]
        if missing_files:
            print(f"Error: The following parameter files do not exist:")
            for f in missing_files:
                print(f"  {f}")
            sys.exit(1)
    
    print(f"Scanning parameter files: {[str(f) for f in parameter_files_list]}")
    print(f"Output directory: {output_dir}")
    print(f"Version tag: {tag}")
    if dry_run:
        print("DRY RUN MODE - No files will be downloaded")
    print()
    
    # Create downloader and run
    downloader = CVMFSFileDownloader(
        output_dir=output_dir,
        tag=tag,
        dry_run=dry_run,
        max_workers=workers,
        verbose=verbose
    )
    
    try:
        success_count, error_count = downloader.download_files(parameter_files_list)
        print(f"\nDownload session '{tag}' completed successfully!")
        
        if not dry_run:
            # Show summary
            versioned_dir = output_dir / tag
            metadata_file = versioned_dir / "metadata.json"
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                summary = metadata.get("summary", {})
                print(f"\nSummary:")
                print(f"  Total files: {summary.get('total_files', 0)}")
                print(f"  Downloaded: {summary.get('downloaded', 0)}")
                print(f"  Hard-linked: {summary.get('hardlinked', 0)}")
                print(f"  Already existed: {summary.get('already_existed', 0)}")
                print(f"  Errors: {summary.get('errors', 0)}")
    
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError during download: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    download_cvmfs_files()
