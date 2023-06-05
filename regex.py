class Regex:
    r1 = r"^(?P<position>left|right|lower)? (?P<location>.*(?= (?P<injury>injury) )).*((?P<time>.*))$"
    r2 = r"^(?P<injury>\w+)(?: (?P<internal>\w+) (?:in))?.*(?P<position>left|right|lower) (?P<location>\w+) ((?P<time>.*))$"
    r3 = r"^(?!.*(?:left|right|injury|lower|returned))(?P<injury>\w+\b(?: of \w+)?)(?: protocols)? (?:(?P<location>\w+) )?((?P<time>.*))$"
    r4 = r"^returned to lineup$"
    re_injury = (r1, r2, r3, r4)
