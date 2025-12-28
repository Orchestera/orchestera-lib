import importlib
import sys

import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m orchestera.entrypoints.run <entrypoint_class>")
        sys.exit(1)

    entrypoint_class_path = sys.argv[1]
    module_path, class_name = entrypoint_class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    entrypoint_cls = getattr(module, class_name)

    entrypoint_cls.initialize_and_run()


if __name__ == "__main__":
    main()
