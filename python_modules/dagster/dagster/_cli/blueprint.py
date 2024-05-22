import json
from importlib import import_module
from pathlib import Path
from typing import Sequence

import click

from dagster import _check as check
from dagster._core.blueprints.load_from_yaml import YamlBlueprintsLoader
from dagster._core.definitions.load_assets_from_modules import find_objects_in_module_of_types


@click.group(name="blueprint")
def blueprint_cli():
    """Commands for working with Dagster blueprints."""


@blueprint_cli.command()
@click.option(
    "--loader-module",
    type=click.STRING,
    help="Path of Python module that contains YamlBlueprintsLoader objects",
    required=True,
)
@click.option(
    "--vscode-folder-path",
    type=click.STRING,
    help="Path to the vscode workspace folder.",
    required=True,
)
def configure_vscode(loader_module: str, vscode_folder_path: str):
    loaders = load_blueprints_loaders_from_module_path(loader_module)
    check.invariant(len(loaders) > 0, "No YamlBlueprintsLoader objects found in the module")

    vscode_folder_path_absolute = Path(vscode_folder_path).absolute()
    dot_vscode_path = vscode_folder_path_absolute / ".vscode"
    check.invariant(
        dot_vscode_path.exists(),
        f"Could not find a .vscode directory in {dir} or any of its parents.",
    )

    recommend_yaml_extension(dot_vscode_path)

    for loader in loaders:
        # Write schema
        schema_path = loader.path / "dagster.autogenerated.schema.json"
        blueprint_type = loader.per_file_blueprint_type
        schema_path.write_text(json.dumps(blueprint_type.model_json_schema(), indent=2))
        click.echo(f"Wrote schema for {blueprint_type.__name__} to {schema_path}")

        add_yaml_schema_to_settings(dot_vscode_path, loader.path, schema_path)


def load_blueprints_loaders_from_module_path(module_path: str) -> Sequence[YamlBlueprintsLoader]:
    import sys

    sys.path.append(".")  # hack for testing. take this out.

    module = import_module(module_path)
    return list(find_objects_in_module_of_types(module, YamlBlueprintsLoader))


def recommend_yaml_extension(dot_vscode_path: Path) -> None:
    extensions_json = dot_vscode_path / "extensions.json"
    if not extensions_json.exists():
        click.echo(f"{extensions_json} does not exist. Creating...")
        extensions_json.write_text("{}")

    yaml_extension_id = "redhat.vscode-yaml"
    try:
        extensions_data = json.loads(extensions_json.read_text())
        extensions_data["recommendations"] = extensions_data.get("recommendations", [])
        if "redhat.vscode-yaml" not in extensions_data["recommendations"]:
            extensions_data["recommendations"].append(yaml_extension_id)
        extensions_json.write_text(json.dumps(extensions_data, indent=2))
        click.echo(f"Updated {extensions_json} to recommend the {yaml_extension_id} extension")
    except:
        click.echo(
            f"Could not automatically update {extensions_json}. Please manually add {yaml_extension_id} as a recommended extension if desired."
        )


def add_yaml_schema_to_settings(dot_vscode_path: Path, yaml_dir: Path, schema_path: Path) -> None:
    settings_json = dot_vscode_path / "settings.json"
    if not settings_json.exists():
        click.echo(f"{settings_json} does not exist. Creating...")
        settings_json.write_text("{}")

    settings_data = json.loads(settings_json.read_text())
    settings_data["yaml.schemas"] = settings_data.get("yaml.schemas", {})
    pattern = f"{yaml_dir}/**/*.yaml"
    yaml_schemas = settings_data["yaml.schemas"]
    yaml_schemas[str(schema_path)] = [pattern]

    settings_json.write_text(json.dumps(settings_data, indent=2))
    click.echo(f"Updated {settings_json} to register {schema_path} with {pattern} files")
    click.echo("You may need to reload the vscode window for changes to take effect.")
