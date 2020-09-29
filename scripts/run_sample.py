import sys

sys.path.append(".") # needed for importing the config, which is not part of the library
from data_access.sample_module import SampleClass
from config import get_config

if __name__ == "__main__":

    c = get_config()
    print(SampleClass().sample_method("AppliedAI"))
    print("Your new library project data_access is waiting for you!")
    print(f"The related data can be stored in {c.data}")
    print("Try running your first build with tox")
