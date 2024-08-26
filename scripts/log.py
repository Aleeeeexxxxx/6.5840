import json
import argparse


class DebugLogEntry:
    template = "<table class='tableStyle'><td>{}</td>"

    def __init__(self, log: str) -> None:
        self.log = log

    def gen(self, max_id: int):
        return DebugLogEntry.template.format(self.log)


class RoleLogEntry:
    template = "<table class='tableStyle'><td>{}</td>{}</table>"

    def __init__(self, time: str, id: str, log: str) -> None:
        self.id = int(id)
        self.log = log
        self.time = time

    def gen(self, max_id: int):
        tdClass = 'td-id'
        if self.log.startswith(" apply message"):
            tdClass = 'td-commit'

        body = ""
        for i in range(max_id+1):
            if self.id == i:
                body += "<td class='{}'>{}</td>".format(tdClass, self.log)
            else:
                body += "<td class='td-id'></td>"
        return RoleLogEntry.template.format(self.time, body)


def createEntry(msg: str):
    if not msg.startswith("2024-08-"):
        return DebugLogEntry(msg), -1

    try:
        items = msg.split("\t")
        # print(items)
        time = items[0]

        fields = items[4]
        fields_json = json.loads(fields)
        id = fields_json["me"]

        log = items[3] + " " + items[4]
    except Exception as e:
        print("wrong entry detected, msg= {}, items={}".format(msg, items))
        return DebugLogEntry(msg), -1

    return RoleLogEntry(time, id, log), id


class Reader:
    def __init__(self, path) -> None:
        self.path = path
        self.entries = []
        self.max_id = -1

    def parse(self):
        with open(self.path) as f:
            while True:
                line = f.readline()
                if line == "":
                    break
                entry, id = createEntry(line)
                if id > self.max_id:
                    self.max_id = id
                self.entries.append(entry)
        return self

    def gen(self):
        body = ""
        for i in self.entries:
            body += i.gen(self.max_id)
        body = "<!DOCTYPE html><body>{}</body>".format(body)
        return body + style()


def style():
    ret = r".tableStyle{width: 100%;word-wrap:bread-word;word-break:break-all;table-layout:fixed;}"
    ret += r" .td-id {font-size:20px;height: auto;border: 1px solid #1f8cfa;}"
    ret += r" .td-commit {font-size:20px;height: auto;border: 1px solid #1f8cfa; background-color: rgba(165, 42, 42, 0.3);}"
    return "<style>{}<\style>".format(ret)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--log", '-l')
    parser.add_argument("--out", '-o')

    args = parser.parse_args()

    ret = Reader(args.log).parse().gen()
    with open(args.out, 'w') as f:
        f.write(ret)


if __name__ == "__main__":
    main()