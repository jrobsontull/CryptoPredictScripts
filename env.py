class Env:
    def __init__(self, envLocation):
        self.env = envLocation
        self.contents = dict()

        with open(self.env, "r") as f:
            for line in f:
                tmp = line.split(" = ")
                self.contents[tmp[0].strip()] = tmp[1].strip().strip("\n")
