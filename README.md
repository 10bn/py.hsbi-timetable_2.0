## Installation

1. **Clone the Repository**:
    - Clone this repository to your local machine using the following command:
        ```bash
        git clone https://github.com/your-username/repo-name.git
        cd repo-name
        ```

2. **Run the Setup Script**:
    - Run the provided `setup.sh` script to install dependencies, set up the virtual environment using Poetry, and copy configuration files:
        ```bash
        ./setup.sh
        ```

    This script will:
    - Check if Poetry is installed, and install it if necessary.
    - Install all project dependencies using Poetry.
    - Set up the virtual environment and activate it.
    - Copy the sample configuration file to the correct location if it doesn't already exist.

3. **Activate the Virtual Environment**:
    - If not automatically activated, you can activate the Poetry-managed virtual environment manually with:
        ```bash
        poetry shell
        ```

    You can also run individual commands within the virtual environment using:
    ```bash
    poetry run <command>
    ```

4. **Configure Application**:
    - The setup process will copy a sample configuration file if `config/config.yaml` does not exist. You can modify this configuration file as needed.
        ```bash
        cp config/config-sample.yaml config/config.yaml
        ```

5. **Running the Application**:
    - To start the application after setup, run:
        ```bash
        poetry run python src/main.py
        ```

---

This update to the `README.md` explains how to use the `setup.sh` script, which manages dependencies via Poetry. It ensures that users have clear steps to follow for installing dependencies, setting up configurations, and running the application.
