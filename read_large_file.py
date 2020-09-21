#!/usr/bin/python

class ReadLargeFile:

    def __init__(self):
        self.lines = []

    def read_one_line(self, f):
        # Read one line and remove extra spaces before and after
        line = f.readline()
        return line

    def write_output(self, output_file):
        of = open(output_file, 'w')

        for line in self.lines:
            of.write(line)

        of.close()

    def main(self, input_file, output_file, no_lines):

        # Open the input file
        f = open(input_file, 'rt')

        for line_index in range(1, no_lines):
            self.lines.append(self.read_one_line(f))

        self.write_output(output_file)

        # Close the input file
        f.close()

if __name__ == "__main__":
    import sys

    input_file = str(sys.argv[1])
    output_file = str(sys.argv[2])
    no_lines = int(sys.argv[3])

    bc = ReadLargeFile()
    bc.main(input_file, output_file, no_lines)