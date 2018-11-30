import xml.etree.cElementTree as et
import pandas as pd
import os
import sys

entity_map = {
    'Tags' : {'Id':[],'TagName':[],'Count':[],'ExcerptPostId':[],'WikiPostId':[]},
    'PostHistory': {'Id':[],'PostHistoryTypeId':[],'PostId':[],'RevisionGUID':[],'CreationDate':[],'UserId':[],'Text':[]},
    'Post': {'Id':[],'PostTypeId':[],'CreationDate':[],'Score':[],'ViewCount':[],'Body':[],'OwnerUserId':[],'LastActivityDate':[],
                'Title':[],'Tags':[],'AnswerCount':[],'CommentCount':[],'FavoriteCount':[],'ClosedDate':[]},
    'User': {'Id':[],'Reputation':[],'CreationDate':[],'DisplayName':[],'LastAccessDate':[],'WebsiteUrl':[],'Location':[],
                'AboutMe':[],'Views':[],'UpVotes':[],'DownVotes':[],'AccountId':[]},
    'Votes': {'Id':[],'PostId':[],'VoteTypeId':[],'CreationDate':[]},
    'Badges': {'Id':[],'UserId':[],'Name':[],'Date':[],'Class':[],'TagBased':[]},
    'Comments': {'Id':[],'PostId':[],'Score':[],'Text':[],'CreationDate':[],'UserId':[]},
    'PostLinks': {'Id':[],'CreationDate':[],'PostId':[],'RelatedPostId':[],'LinkTypeId':[]}        
}

def main(inputs,output,entity_name):

    valid_entity_names = list(entity_map.keys())
    if entity_name in entity_map:
        entity = entity_map[entity_name]
    else: 
        print(" Please enter entity name in "+ str(valid_entity_names))

    for subdirs,dirs,files in os.walk(inputs+entity_name+'/'):
        for file in files:
            parsedXML = et.parse(inputs+entity_name+'/'+file+'')
            for node in parsedXML.getroot():
                for i in entity:
                    entity[i].append(node.attrib.get(i))
            dfcols = list(entity.keys())
            df_xml = pd.DataFrame(columns=dfcols)
            for col in dfcols:
                df_xml[col]=pd.Series(entity[col])
            name_1 = file.split('.')
            site_name = name_1[0].split('_')
            df_xml['SiteName']=site_name[1]
            out_name  = name_1[0] + '.csv'
            break
            df_xml.to_csv(output+entity_name+'/'+out_name+'',sep=',',index=False,index_label=False,columns=dfcols.append('SiteName'))
            print(out_name+' done')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    entity_name = sys.argv[3]
    main(inputs,output,entity_name)

#How to run this python from command line?
#Sample command
#python xml_to_csv.py /Users/ritesh/stackexchange-analytics/ETL/ /Users/ritesh/stackexchange-analytics/ETL/ Tags
