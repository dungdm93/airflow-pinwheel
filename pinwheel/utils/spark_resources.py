from dataclasses import dataclass


@dataclass(frozen=True)
class Resource:
    cores: int | None = None
    core_request: str | None = None
    core_limit: str | None = None
    memory: str | None = None
    memory_overhead: str | None = None


NONE = Resource()
MICRO = Resource(cores=1, core_request="1", memory="4g", core_limit="2")
SMALL = Resource(cores=2, core_request="1", memory="8g", core_limit="3")
MEDIUM = Resource(cores=4, core_request="1", memory="16g", core_limit="5")
# core_request = cores / 2
# core_limit = cores * 1.25
STANDARD = Resource(cores=8, memory="32g", core_request="4", core_limit="10")
LARGE = Resource(cores=16, memory="64g", core_request="8", core_limit="20")
HUGE = Resource(cores=24, memory="96g", core_request="12", core_limit="30")
GIANT = Resource(cores=32, memory="128g", core_request="16", core_limit="40")
