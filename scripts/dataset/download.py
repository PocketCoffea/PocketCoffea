import os
import json
import argparse
import pprint

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Download files specificed in a dataset.json'
    )
    parser.add_argument('-i', '--input', default=r'metadata/dataset.json', help='')
    parser.add_argument('-d', '--dir', help='Storage directory', required=True)
    parser.add_argument(
        '-o', '--output', default=r'metadata/dataset_local.json', help=''
    )
    parser.add_argument('--download', help='Bool', action='store_true')

    args = parser.parse_args()

    # load dataset
    with open(args.input) as f:
        sample_dict = json.load(f)

    print("Storage dir:")
    print("   ", os.path.abspath(args.dir))

    # Download instance
    @python_app
    def down_file(fname, out, ith=None):
        if ith is not None:
            print(ith)
        os.system("xrdcp -P " + fname + " " + out)
        return 0

    # Setup multithreading
    config = Config(executors=[ThreadPoolExecutor(max_threads=8)])
    parsl.load(config)

    # Write futures
    out_dict = {}  # Output filename list
    run_futures = []  # Future list
    for key in sorted(sample_dict.keys()):
        new_list = []
        # print(key)
        for i, fname in enumerate(sample_dict[key]):
            if i % 5 == 0:
                # print some progress info
                ith = f'{key}: {i}/{len(sample_dict[key])}'
            else:
                ith = None
            out = os.path.join(
                os.path.abspath(args.dir), fname.split("//")[-1].lstrip("/")
            )
            new_list.append(out)
            if args.download:
                if os.path.isfile(out):
                    'File found'
                else:
                    x = down_file(fname, out, ith)
                    run_futures.append(x)
        out_dict[key] = new_list

    for i, r in enumerate(run_futures):
        r.result()

    print("Writing files to {}".format(args.output))
    with open(args.output, 'w') as fp:
        json.dump(out_dict, fp, indent=4)
