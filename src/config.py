from configparser import ConfigParser
def config(filename="database.ini",section="postgresql"):
    parser=ConfigParser()
    parser.read(filename)
    db= {}
    if parser.has_section(section):
        params=parser.items(section)
        for param in params:
            db[param[0]]=param[1]
    else:
        raise Exception('section {0} is not found in file {1}'.format(section,filename))
    print(db)
    
config()