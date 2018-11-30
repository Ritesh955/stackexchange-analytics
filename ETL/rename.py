import glob, os

def add_site_to_filename(dir, pattern):
    dir_path = os.path.dirname(dir)
    parent_folder= dir_path.split(os.sep)[-1].split(".")[0]
    for pathAndFilename in glob.iglob(os.path.join(dir, pattern)):
        title, ext = os.path.splitext(os.path.basename(pathAndFilename))
        os.rename(pathAndFilename, os.path.join(dir, title+ '_' +parent_folder+ ext))
 
 # Subject name already appended       
 # add_site_to_filename("/Users/ritesh/Downloads/ai/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/physics/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/datascience/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/chemistry/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/biology/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/english/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/economics/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/movies/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/music/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/politics/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/sports/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/games/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/math/","*.xml")
 # add_site_to_filename("/Users/ritesh/Downloads/gaming/","*.xml")
