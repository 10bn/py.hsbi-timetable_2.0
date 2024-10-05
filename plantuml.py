# generate_diagram.py
from py2puml.py2puml import py2puml

if __name__ == '__main__':
    # Generate PlantUML content
    puml_content = ''.join(py2puml('src', 'py_hsbi_timetable_2_0'))
    
    # Print to terminal
    print(puml_content)
    
    # Write to file
    with open('diagram.puml', 'w', encoding='utf8') as puml_file:
        puml_file.writelines(puml_content)
