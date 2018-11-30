import xml.etree.cElementTree as et
import pandas as pd
import os
import sys
import fastparquet


entity_map = {
    'Tags' : {'Id':[],'TagName':[],'Count':[],'ExcerptPostId':[],'WikiPostId':[]},
    'PostHistory': {'Id':[],'PostHistoryTypeId':[],'PostId':[],'RevisionGUID':[],'CreationDate':[],'UserId':[],'Text':[],'CloseReasonId':[]},
    'Posts': {'Id':[],'PostTypeId':[],'ParentID':[],'AcceptedAnswerId':[],'CreationDate':[],'Score':[],'ViewCount':[],'Body':[],'OwnerUserId':[],'LastEditorUserId':[],'LastEditorDisplayName':[],
              'LastEditDate':[],'LastActivityDate':[],'Title':[],'Tags':[],'AnswerCount':[],'CommentCount':[],'FavoriteCount':[],'ClosedDate':[]},
    'Users': {'Id':[],'Reputation':[],'CreationDate':[],'DisplayName':[],'LastAccessDate':[],'WebsiteUrl':[],'Location':[],
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
        print(" Please enter a valid entity name from  "+ str(valid_entity_names))

    for subdirs,dirs,files in os.walk(inputs+entity_name+'/'):
        for file in files:
            print("processing :"+ file)
            parsedXML = et.parse(inputs+entity_name+'/'+file)
            for i in entity.keys():
                entity[i]= []
            for node in parsedXML.getroot():
                for i in entity.keys():
                    entity[i].append(node.attrib.get(i))
            dfcols = list(entity.keys())
            df_xml = pd.DataFrame(columns=dfcols)
            for col in dfcols:
                 df_xml[col]=pd.Series(entity[col])
            name_1 = file.split('.')
            site_name = name_1[0].split('_')
            df_xml['sites']=site_name[1]
            out_name  = name_1[0]+'.parquet'
            df_xml.to_parquet(output+'/'+out_name, compression='gzip',engine='pyarrow')
            print(out_name+' done')
        break

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    entity_name = sys.argv[3]
    main(inputs,output,entity_name)

##How to run this file?
#Sample command
#manually creare folder called parquet
#python xml_to_csv.py /Users/ritesh/Downloads/XML/data/ ./parquet/ Comments