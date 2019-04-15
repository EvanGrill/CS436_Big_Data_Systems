from sys import argv

def main():
    if len(argv) < 3:
        print("Please include a file to sanitize and an output file.")
        print("sanitize.py [in] [out]")
    else:
        input = open(argv[1], "r")
        output = open(argv[2], "w")
        for line in input:
            if len(line) > 12:
                output.write(line)
    

if __name__ == "__main__":
    main()