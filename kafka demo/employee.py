import json
from typing import Optional

class Employee:

    def __init__(self, emp_id: int = 0, first_name: str = '', last_name: str = '', dob: Optional[str] = '', city: str = '', action: str = ''):
        self.emp_id = emp_id
        self.first_name = first_name
        self.last_name = last_name
        self.dob = dob
        self.city = city
        self.action = action

    @staticmethod
    def from_csv_line(line):
        return Employee(line[0], line[1], line[2], line[3].isoformat(), line[4], line[5])

    def to_json(self):
        return json.dumps(self.__dict__)
