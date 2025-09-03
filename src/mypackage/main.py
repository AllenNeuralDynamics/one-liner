def beep_boop(msg: str) -> str:
    return msg.replace('o', '0')    


def main() -> None:
    print(beep_boop("Howdy"))


if __name__ == "__main__":
    main()
