import os


def merge_files_in_directory(input_dir: str, output_file: str) -> None:
    """
    Merges all Python files in the specified directory into a single text file.

    Args:
        input_dir (str): Path to the directory containing files to merge.
        output_file (str): Path to the output file where the merged content will be written.
    """
    with open(output_file, "w", encoding="utf-8") as outfile:
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                if file.endswith(".py"):  # Only merge Python files
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, "r", encoding="utf-8") as infile:
                            outfile.write(
                                f"# {file_path}\n"
                            )  # Add file path at the top of the file's content
                            outfile.write(
                                infile.read()
                            )  # Write the content of the file
                            outfile.write("\n\n")  # Add some spacing between files
                    except Exception as e:
                        print(f"Failed to read file {file_path}: {e}")
                        continue

    print(f"All Python files from {input_dir} have been merged into {output_file}")


# Set the input directory and output file path
input_directory = "src"
output_file = "merged_output.txt"

# Run the merging process
merge_files_in_directory(input_directory, output_file)
