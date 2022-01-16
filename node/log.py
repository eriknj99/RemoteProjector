from termcolor import colored

info_enable = True
debug_enable = True
error_enable = True

log_cache = []

def get_log_cache()->str:
    out = ""
    for log in log_cache:
        out+=log
    return out

def clear():
    print('\033c')

def info(out, end="\n"):
    if(info_enable):
        output = f"[{colored('INFO ', 'blue')}]\t{out}{end}"
        log_cache.append(output)
        print(output,end="")

def debug(out, end="\n"):
    if(debug_enable):
        output = f"[{colored('DEBUG', 'green')}]\t{out}{end}"
        log_cache.append(output)
        print(output,end="")

def error(out, end="\n"):
    if(debug_enable):
        output = f"[{colored('ERROR', 'red')}]\t{out}{end}"
        log_cache.append(output)
        print(output,end="")

