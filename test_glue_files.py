import pytest
import os

def pytest_generate_tests(metafunc):
    # Collect all Python scripts in the 'scripts/' directory
    script_dir = 'glue_files/'
    script_files = [f for f in os.listdir(script_dir) if f.endswith('.py')]
    metafunc.parametrize('script_path', script_files)

def test_syntax_error(script_path):
    script_path = os.path.join('glue_files', script_path)
    
    try:
        with open(script_path, 'r') as file:
            script_content = file.read()
        # Attempt to compile the script content
        compile(script_content, script_path, 'exec')
    except SyntaxError as e:
        pytest.fail(f'Syntax error in script {script_path}: {e}')
