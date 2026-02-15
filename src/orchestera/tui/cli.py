import click

from orchestera.tui.app import OrchesteraTuiApp


@click.command()
def main() -> None:
    """Launch the Orchestera TUI."""
    OrchesteraTuiApp().run()


if __name__ == "__main__":
    main()
