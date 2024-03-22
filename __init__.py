import os
import capnp
from typing import TYPE_CHECKING

CEREAL_PATH = os.path.dirname(os.path.abspath(__file__))
capnp.remove_import_hook()

if TYPE_CHECKING:
  # ruff: noqa: F401
  import cereal.gen.py.car_capnp as car
  import cereal.gen.py.log_capnp as log
  import cereal.gen.py.custom_capnp as custom
else:
  log = capnp.load(os.path.join(CEREAL_PATH, "log.capnp"))
  car = capnp.load(os.path.join(CEREAL_PATH, "car.capnp"))
  custom = capnp.load(os.path.join(CEREAL_PATH, "custom.capnp"))