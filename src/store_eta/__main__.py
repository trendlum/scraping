if __package__:
    from .cli import main
else:
    from pathlib import Path
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from store_eta.cli import main


if __name__ == "__main__":
    main()
