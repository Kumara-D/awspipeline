import pytest

def test_syntax_error():
    script_path = 'DMDOMITINV00091125.py'
    
    try:
        with open(script_path, 'r') as file:
            script_content = file.read()
        # Attempt to compile the script content
        compile(script_content, script_path, 'exec')
    except SyntaxError as e:
        pytest.fail(f'Syntax error in script {script_path}: {e}')