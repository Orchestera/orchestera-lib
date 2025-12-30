from orchestera.entrypoints.base_entrypoint import BaseEntryPoint, StringArg


class SparklithEntryPoint(BaseEntryPoint):
    """Implements SparkeumEntryPoint"""

    application_name = StringArg(required=True, tooltip="Name of the application")
    # image = StringArg(required=True, tooltip="Image to use for the application")

    def run(self):
        """
        Run the entry point using the parsed CLI arguments.
        """
        raise NotImplementedError
