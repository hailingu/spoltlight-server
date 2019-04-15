def mkdir(path):
    import os
    path = path.strip()
    path = path.rstrip("\\")
    existed = os.path.exists(path)
    if not existed:
        os.makedirs(path)
        return True
    else:
        return False

class Singleton(type):
    _instance = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instance:
            cls._instance[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instance[cls].__init__(*args, **kwargs)
    
        return cls._instance[cls]