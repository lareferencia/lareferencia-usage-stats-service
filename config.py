import configparser

def read_ini(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    #for section in config.sections():
    #    for key in config[section]:
    #        print((key, config[section][key]))
    
    return config