import os
import json

class Logger():
    def __init__(self, path):
        self.path = path
        # Check file integrity
        if not self.get_log():
            self.reset()

    def get_log(self):
        try:
            with open(self.path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return None

    def write_log(self, log):
        parent_dir = os.path.split(self.path)[0]
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
        with open(self.path, 'w') as file:
            json.dump(log, file, indent=4)

    def add_log(self, data):
        log = self.get_log()
        key, val = data
        if log:
            if key in log['posts'] and val not in log['posts'][key]:
                log['posts'][key].append(val)
            else:
                log['posts'][key] = [val]
            self.write_log(log)

    def update_status(self):
        log = self.get_log()
        if not log['posts']:
            log['status'] = 'stopped'
        else:
            log['status'] = 'paused'
        self.write_log(log)

    def pop_log(self, data):
        url, link = data
        log = self.get_log()
        if url in log['posts']:
            log['posts'][url].remove(link)
            if url in log['pushed_posts']:
                log['pushed_posts'][url].append(link)
            else:
                log['pushed_posts'][url] = [link]
            if not log['posts'][url]:
                del log['posts'][url]
        self.write_log(log)
        self.update_status()

    def reset(self):
        self.write_log({'status': 'stopped', 'posts': {}, "pushed_posts": {}})
