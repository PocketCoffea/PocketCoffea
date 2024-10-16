import os
from pathlib import Path

import law
import luigi

law.contrib.load("wlcg")


class BaseTask(law.Task):
    version = luigi.Parameter(description="Version of the task", default="")

    @property
    def base_store(self) -> Path:
        """The base path where all output files of tasks are stored.

        :return: Environment variable ANALYSIS_STORE
        :rtype: Path
        """
        return Path(os.environ["ANALYSIS_STORE"])

    @property
    def version_store(self) -> Path:
        """The base path where all output files of tasks are stored
        for a specific version.

        :return: base_store for specific version
        :rtype: Path
        """
        return Path(self.base_store, self.version)

    def local_path(self, *path: str) -> Path:
        """Return path to a location in the local store.
        Is always prepended with environment variable $ANALYSIS_STORE.
        Pass multiple path parts as separate arguments.

        :return: joined ANALYSIS_STORE with `store_parts` and arguments
        :rtype: str
        """
        parts = [str(p) for p in self.store_parts() + path]
        # NOTE: should we use law config `local_fs` instead of os.environ?
        return Path(os.environ["ANALYSIS_STORE"], *parts)

    def local_file_target(self, *path: str) -> law.LocalFileTarget:
        """Return a LocalFileTarget for the given path(s).
        Pass multiple path parts as separate arguments.

        :return: LocalFileTarget for the given path
        :rtype: law.LocalFileTarget
        """
        return law.LocalFileTarget(self.local_path(*path))

    def local_directory_target(self, *path: str) -> law.LocalDirectoryTarget:
        """Return a LocalDirectoryTarget for the given path(s).
        Pass multiple path parts as separate arguments.

        :return: LocalDirectoryTarget for the given path
        :rtype: law.LocalDirectoryTarget
        """
        return law.LocalDirectoryTarget(self.local_path(*path))

    def wlcg_path(self, *path: str) -> Path:
        """Return path to a location in the WLCG store.
        Pass multiple path parts as separate arguments.

        :return: joined store_parts and arguments
        :rtype: str
        """
        parts = [str(p) for p in self.store_parts() + path]
        return Path(*parts)

    def wlcg_file_target(self, *path: str, **kwargs) -> str:
        """Return a WLCGFileTarget for the given path(s).
        Pass multiple path parts as separate arguments.
        Will be prepended with the store's base path set in law.cfg.

        :return: WLCGFileTarget for the given path
        :rtype: str
        """
        return law.wlcg.WLCGFileTarget(self.wlcg_path(*path), **kwargs)

    def wlcg_directory_target(self, *path: str, **kwargs) -> str:
        """Return a WLCGDirectoryTarget for the given path(s).
        Pass multiple path parts as separate arguments.
        Will be prepended with the store's base path set in law.cfg.

        :return: WLCGDirectoryTarget for the given path
        :rtype: str
        """
        return law.wlcg.WLCGDirectoryTarget(self.wlcg_path(*path), **kwargs)

    def store_parts(self) -> tuple[str]:
        """Tuple of parts that get added to the store path (local/wlcg).
        Can be overridden in subclasses to add more parts.

        :return: Task class name and version
        :rtype: tuple[str]
        """
        parts = (self.__class__.__name__,)
        if self.version is not None:
            parts = (self.version, *parts)

        return parts
