import pytest
import os

def test_syntax_error():
    scripts_dir = 'glue_files'
    scripts = [f for f in os.listdir(scripts_dir) if f.endswith('.py')]
    
    for script in scripts:
        script_path = os.path.join(scripts_dir, script)
        try:
            with open(script_path, 'r') as file:
                script_content = file.read()
            compile(script_content, script_path, 'exec')
        except SyntaxError as e:
            pytest.fail(f'Syntax error in script {script_path}: {e}')

if __name__ == "__main__":
    pytest.main()
