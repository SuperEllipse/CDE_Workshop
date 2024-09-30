import subprocess
import sys
import importlib
import pkg_resources
import configparser
import os

def install_and_import(package, version=None, index_url=None):
    """
    Install and import a package dynamically.

    Args:
        package (str): The name of the package to install.
        version (str, optional): The specific version to install. Defaults to None.
        index_url (str, optional): The URL of the package index. Defaults to None.

    Returns:
        module: The imported module after installation.
    """
    try:
        # Check if the package is already installed
        if version:
            pkg_resources.require(f"{package}=={version}")
        else:
            pkg_resources.require(package)
        print(f"Package '{package}' is already installed.")
        
    except pkg_resources.DistributionNotFound:
        # Package is not installed, so install it
        if version:
            package_with_version = f"{package}=={version}"
        else:
            package_with_version = package
        
        print(f"Installing package '{package_with_version}'...")

        # Prepare the pip install command
        install_command = [sys.executable, "-m", "pip", "install", package_with_version]

        # Add index URL if specified
        if index_url:
            install_command.extend(["--index-url", index_url])

        # Run pip install
        subprocess.run(install_command, check=True)
        print(f"Package '{package_with_version}' installed successfully.")
    
    except pkg_resources.VersionConflict:
        print(f"Version conflict for package '{package}'. Installing specified version '{version}'...")
        install_command = [sys.executable, "-m", "pip", "install", f"{package}=={version}"]

        if index_url:
            install_command.extend(["--index-url", index_url])

        subprocess.run(install_command, check=True)
        print(f"Package '{package}=={version}' installed successfully.")

    # Import the package dynamically
    try:
        # Example: Convert package name to module name, if necessary
        # If the package is named with dashes, replace with underscores
        module_name = package.replace("-", "_")
        
        # Import the correct module
        module = importlib.import_module(module_name)
        print(f"Imported '{module_name}' successfully.")
        return module
    except ImportError as e:
        print(f"Failed to import '{module_name}': {e}")
        return None

def get_registry_url(config_file='config.ini'):
    """
    Get the package registry URL from a configuration file.

    Args:
        config_file (str): Path to the configuration file. Defaults to 'config.ini'.

    Returns:
        str: The registry URL.
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"The configuration file '{config_file}' was not found.")
    
    config = configparser.ConfigParser()

    try:
        config.read(config_file)
        registry_url = config.get('Registry', 'url')
        return registry_url
    except configparser.NoSectionError:
        raise ValueError("The configuration file is missing the 'Registry' section.")
    except configparser.NoOptionError:
        raise ValueError("The configuration file is missing the 'url' option in the 'Registry' section.")
    except Exception as e:
        raise Exception(f"An error occurred while reading the configuration file: {e}")

def get_packages_info(config_file='config.ini'):
    """
    Get information about all main packages from a configuration file.

    Args:
        config_file (str): Path to the configuration file. Defaults to 'config.ini'.

    Returns:
        dict: A dictionary where keys are package names and values are dictionaries 
              containing version and submodules for each package.
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"The configuration file '{config_file}' was not found.")

    config = configparser.ConfigParser()

    try:
        config.read(config_file)
        packages_info = {}
        
        for section in config.sections():
            if section != 'Registry':
                version = config.get(section, 'version')
                submodules = config.get(section, 'submodules').split(', ')
                packages_info[section] = {
                    'version': version,
                    'submodules': submodules
                }

        return packages_info

    except Exception as e:
        raise Exception(f"An error occurred while reading the configuration file: {e}")

def import_submodules(package_name, submodules):
    """
    Dynamically import submodules of a package into the local namespace.

    Args:
        package_name (str): The name of the main package.
        submodules (list): List of submodule names to import.

    Returns:
        dict: A dictionary with submodule names as keys and imported modules as values.
    """
    imported_modules = {}
    for submodule in submodules:
        full_module_name = f"{package_name}.{submodule}"
        try:
            # Dynamically import into the local namespace
            module = importlib.import_module(full_module_name)
            globals()[submodule] = module
            print(f"Imported submodule '{full_module_name}' into the local namespace.")
            imported_modules[submodule] = module
        except ImportError as e:
            print(f"Failed to import submodule '{full_module_name}': {e}")
    return imported_modules

# Example usage
if __name__ == "__main__":
    # Configuration file
    config_file = 'config.ini'
    
    try:
        # Get registry URL from configuration
        registry_url = get_registry_url(config_file)

        # Get all main packages info from configuration
        packages_info = get_packages_info(config_file)

        for main_package, info in packages_info.items():
            package_version = info['version']
            submodules = info['submodules']

            # Install the main package with the specified version
            install_and_import(main_package, package_version, index_url=registry_url)

            # Import submodules
            import_submodules(main_package, submodules)
    except Exception as e:
        print(f"Error: {e}")

    # Example usage of imported submodules
    # Accessing the function directly if it exists
    if 'example' in globals():
        if hasattr(example, 'add_two'):
            result = example.add_two(5)
            print(f"example.add_two(5): {result}")

