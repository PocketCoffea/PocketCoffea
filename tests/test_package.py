from __future__ import annotations

import pocket_coffea as m


def test_version():
    assert m.__version__

def main():
    print("Pocket Version:", m.__version__)

if __name__ == "__main__":
    main()
