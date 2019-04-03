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