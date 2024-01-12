from http.client import HTTPResponse
from typing import Any, Iterator
from xml.etree import ElementTree as ET


def parse_xml(data: HTTPResponse) -> Iterator[dict[str, Any]]:
    namespace_map = {}
    cube_level = 0
    cube_tag = None
    current_day_rates = None
    for event, elem in ET.iterparse(data, ["start", "end", "start-ns"]):
        if event == "start":
            if elem.tag == cube_tag:
                if cube_level == 2:
                    e = elem.attrib
                    rate = e["rate"]
                    if "." not in rate:
                        rate = f"{rate}.0"
                    current_day_rates[e["currency"]] = rate
                    cube_level += 1
                elif cube_level == 1:
                    current_day_rates = {"date": elem.attrib["time"]}
                    cube_level += 1
                elif cube_level == 0:
                    cube_level += 1
        elif event == "end":
            if elem.tag == cube_tag:
                cube_level -= 1
                if cube_level == 1:
                    yield current_day_rates
                    current_day_rates = None
        elif event == "start-ns":
            namespace_map[elem[0]] = elem[1]
            if elem[0] == "":
                cube_tag = f"{{{elem[1]}}}Cube"
        else:
            raise ValueError("Unexpected event.", event)
    if current_day_rates:
        raise ValueError(
            f"Unexpected data left, possibly due to malformed XML.\n"
            f"Last parsed data: {current_day_rates}\n"
            f"Cube tag level: {cube_level}"
        )
