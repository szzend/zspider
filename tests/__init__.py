import colorama

def cprint(message:str):
    colorama.init(autoreset=True,strip=True)
    print(colorama.Fore.LIGHTBLUE_EX+message)