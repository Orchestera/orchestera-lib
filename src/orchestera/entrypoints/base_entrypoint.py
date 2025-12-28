from abc import ABC, abstractmethod
import sys
import click
import logging


logger = logging.getLogger(__name__)


class ArgMeta(type):
    """Metaclass for Args"""


class BaseArg(metaclass=ArgMeta):
    """Implements BaseArg"""

    def __init__(self, required, tooltip):
        self.required = required
        self.tooltip = tooltip

    def set_var_name(self, var_name):
        """Set the variable name for the argument"""
        self.var_name = var_name

    def set_cli_arg_value(self, cli_arg_value):
        """Set the CLI argument value for the argument"""
        self.cli_arg_value = cli_arg_value

    def get_cli_arg_value(self):
        """Get the CLI argument value for the argument"""
        return self.cli_arg_value

    def __str__(self):
        return self.cli_arg_value

    def get_click_option(self):
        """This method should be implemented by subclasses to return the click option"""
        raise NotImplementedError("get_click_option method must be implemented")


class StringArg(BaseArg):
    """Implements StringArg"""

    def get_click_option(self):
        return click.option(
            f"--{self.var_name.replace('_', '-')}",
            required=self.required,
            help=self.tooltip,
            type=str,
        )


class BaseEntryPoint(ABC):
    """Abstract base class for entry points."""

    @abstractmethod
    def run(self):
        """Abstract method to run the entry point."""
        raise NotImplementedError

    @classmethod
    def _collect_base_args(cls):
        base_args = {}
        for base_cls in cls.__mro__:
            for key, value in base_cls.__dict__.items():
                if isinstance(value, BaseArg) and key not in base_args:
                    base_args[key] = value
        return base_args

    @classmethod
    def parse_cli_args(cls):

        def cli_entrypoint(**kwargs):
            return kwargs

        base_args = cls._collect_base_args()
        for key, value in base_args.items():
            value.set_var_name(key)
            cli_entrypoint = value.get_click_option()(cli_entrypoint)

        cli_entrypoint = click.command()(cli_entrypoint)
        cli_args = cli_entrypoint.main(args=sys.argv[2:], standalone_mode=False)

        for key, value in base_args.items():
            value.set_cli_arg_value(cli_args[key])

    @classmethod
    def initialize_and_run(cls):
        cls.parse_cli_args()
        instance = cls()

        base_args = cls._collect_base_args()
        for key, value in base_args.items():
            setattr(instance, key, value.get_cli_arg_value())

        logger.info("Parsed CLI arguments")
        for key, value in base_args.items():
            logger.info(f"{key}: {value.get_cli_arg_value()}")

        instance.run()
