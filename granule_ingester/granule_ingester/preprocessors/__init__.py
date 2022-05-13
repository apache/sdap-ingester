import aquarius

PREPROCESSORS = {
    'AQUARIUS': aquarius
}

def get_preprocessor(name):
    return PREPROCESSORS[name].run_preprocess