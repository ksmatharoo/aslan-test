from dataclasses import dataclass
from typing import List, Callable


from dataclasses import dataclass, asdict
from typing import List, Callable


@dataclass
class JobDefinition:
    jobName: str
    dependencyList: List[str]
    alias: str
    dagName: str
    func: Callable

    def to_dict(self):
        data = asdict(self)
        data.pop("func")  # cannot serialize function
        return data