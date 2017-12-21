c = get_config()

c.TerminalIPythonApp.display_banner = False
c.InteractiveShell.confirm_exit = False
c.InteractiveShellApp.log_level = 20
c.InteractiveShellApp.exec_lines = [
    'from pivot import *',
]
