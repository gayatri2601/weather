from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
import asyncio
import aiohttp
from .blob_service import upload_csv_to_blob
import pandas as pd
import os
timeout = aiohttp.ClientTimeout(total=120)

async def fetch_data(session, url,country,region):
    if not url:
        return {"Url":url,'CountryTerritory':country,"Region":region,"Identifier":"","Sender":"","IssuedTime":"","Status":"","MessageType":"",
                "Scope":"","References":"","Language":"","Category":"","ValueName":"","Value":"",
                "Event":"","Headline":"","Urgency":"","Severity":"","Certainty":"","EffectiveTime":"",
                "OnsetTime":"","ExpiresTime":"","AffectedArea":"","Polygon":"",
                "Instruction":"","EventDescription":""}
    async with session.get(url) as response:
        xml_data = await response.text()
        soup = BeautifulSoup(xml_data, 'xml')
        #1.Identifier
        results={}
        results['Url']=url
        results['CountryTerritory']=country
        results['Region']=region 

        #1.Identifier
        Identifier = soup.find('identifier')
        if Identifier is None:
            Identifier = ''
        else:
            Identifier = soup.find('identifier').get_text()
        results['Identifier']=Identifier

        #2.Sender
        Sender = soup.find('sender')
        if Sender is None:
            Sender = ''
        else:
            Sender = soup.find('sender').get_text()
        results['Sender']=Sender

        #3.IssuedTime
        IssuedTime = soup.find('sent')
        if IssuedTime is None:
            IssuedTime = ''
        else:
            IssuedTime = soup.find('sent').get_text()
        results['IssuedTime']=IssuedTime

        #4.Status
        Status = soup.find('status')
        if Status is None:
            Status = ''
        else:
            Status = soup.find('status').get_text()
        results['Status']=Status

        #5.MessageType
        MessageType = soup.find('msgType')
        if MessageType is None:
            MessageType = ''
        else:
            MessageType = soup.find('msgType').get_text()
        results['MessageType']=MessageType

        #6.scope
        Scope = soup.find('scope')
        if Scope is None:
            Scope = ''
        else:
            Scope = soup.find('scope').get_text()
        results['Scope']=Scope

        #7.References
        References = soup.find('references')
        if References is None:
            References = ''
        else:
            References = soup.find('references').get_text()
        results['References']=References
        language = soup.find('language', string=lambda text: text and text.startswith('en'))

        # TODO this used when other than english language is came
        if not language:
            language = soup.find('language')
        #9.Category
        if not language:
            Language = ''
            Category = soup.find('category')
            if Category is None:
                Category = ''
            else:
                Category = soup.find('category').get_text()

            #10.ValueName
            ValueName = soup.find('valueName')
            if ValueName is None:
                ValueName = ''
            else:
                ValueName = soup.find('valueName').get_text()

            #11.Value
            Value = soup.find('value')
            if Value is None:
                Value = ''
            else:
                Value = soup.find('value').get_text()

            #12.Event
            Event = soup.find('event')
            if Event is None:
                Event = ''
            else:
                Event = soup.find('event').get_text()

            #13.Headline
            Headline = soup.find('headline')
            if Headline is None:
                Headline = ''
            else:
                Headline = soup.find('headline').get_text()

            #14.Urgency
            Urgency = soup.find('urgency')
            if Urgency is None:
                Urgency = ''
            else:
                Urgency = soup.find('urgency').get_text()

            #15.Severity
            Severity = soup.find('severity')
            if Severity is None:
                Severity = ''
            else:
                Severity = soup.find('severity').get_text()

            #16.Certainty
            Certainty = soup.find('certainty')
            if Certainty is None:
                Certainty = ''
            else:
                Certainty = soup.find('certainty').get_text()

            #17.EffectiveTime
            EffectiveTime = soup.find('effective')
            if EffectiveTime is None:
                EffectiveTime = ''
            else:
                EffectiveTime = soup.find('effective').get_text()

            #18.OnsetTime
            OnsetTime = soup.find('onset')
            if OnsetTime is None:
                OnsetTime = ''
            else:
                OnsetTime = soup.find('onset').get_text()

            #19.ExpiresTime
            ExpiresTime = soup.find('expires')
            if ExpiresTime is None:
                ExpiresTime = ''
            else:
                ExpiresTime = soup.find('expires').get_text()

            #20.AffectedArea
            AffectedArea = soup.find('areaDesc')
            if AffectedArea is None:
                AffectedArea = ''
            else:
                AffectedArea = soup.find('areaDesc').get_text()

            #21.Polygon
            Polygon = ''

            #22.Instruction
            Instruction = soup.find('instruction')
            if Instruction is None:
                Instruction = ''
            else:
                Instruction = soup.find('instruction').get_text()

            EventDescription = soup.find('description')
            if EventDescription is None:
                EventDescription = ''
            else:
                EventDescription = soup.find('description').get_text()
        else:
            Language=language.text
            Category=language.find_next('category')
            if Category:
                Category=Category.text

            ValueName=language.find_next('valueName')
            if ValueName:
                ValueName=ValueName.text

            Value=language.find_next('value')
            if Value:
                Value=Value.text

            Event=language.find_next('event')
            if Event:
                Event=Event.text

            Headline=language.find_next('headline')
            if Headline:
                Headline=Headline.text

            Urgency=language.find_next('urgency')
            if Urgency:
                Urgency=Urgency.text

            Severity=language.find_next('severity')
            if Severity:
                Severity=Severity.text

            Certainty=language.find_next('certainty')
            if Certainty:
                Certainty=Certainty.text

            EffectiveTime=language.find_next('effective')
            if EffectiveTime:
                EffectiveTime=EffectiveTime.text

            OnsetTime=language.find_next('onset')
            if OnsetTime:
                OnsetTime=OnsetTime.text

            ExpiresTime=language.find_next('expires')
            if ExpiresTime:
                ExpiresTime=ExpiresTime.text

            AffectedArea=language.find_next('areaDesc')
            if AffectedArea:
                AffectedArea=AffectedArea.text

            Polygon=language.find_next('polygon')
            if Polygon:
                Polygon=Polygon.text

            Instruction=language.find_next('instruction')
            if Instruction:
                Instruction=Instruction.text

            EventDescription=language.find_next('description')
            if EventDescription:
                EventDescription=EventDescription.text          

        results['Language']=Language
        results['Category']=Category
        results['ValueName']=ValueName
        results['Value']=Value
        results['Event']=Event
        results['Headline']=Headline
        results['Urgency']=Urgency
        results['Severity']=Severity
        results['Certainty']=Certainty
        results['EffectiveTime']=EffectiveTime
        results['OnsetTime']=OnsetTime
        results['ExpiresTime']=ExpiresTime
        results['AffectedArea']=AffectedArea
        results['Polygon']=Polygon
        results['Instruction']=Instruction
        results['EventDescription']=EventDescription
        return results

async def demo_main():
    response=requests.get("https://severeweather.wmo.int/v2/json/wmo_all.json")
    country_response=requests.get("https://severeweather.wmo.int/v2/json/wmo_member.json")
    country_datas=country_response.json()
    country_data={}
    for x in country_datas:
        country_data[str(x["ra"])]={}
        for y in x["members"]:
            country_data[str(x["ra"])][y["mid"]]=y["name"]
    ra_arr = ["Region I - Africa", "Region II - Asia", "Region III - South America", "Region IV - North America, Central America and the Caribbean", "Region V - South West Pacific", "Region VI - Europe"];
    results_data = {"Url":[],'CountryTerritory':[], 'Region':[]}
    
    data=response.json()
    items=data.get("items")
    urls=[]
    for x in items:
        if "capURL" in x:
            name=x["capURL"]
            results_data['Url'].append(f"https://8xieiqdnye.execute-api.us-west-2.amazonaws.com/swic/capURL/{name}")
        elif "url" in x:
            name=x["url"]
            results_data['Url'].append(f"https://cvzxdcwxid.execute-api.us-west-2.amazonaws.com/swic/url/{name}")
        
        else:
            results_data['Url'].append("")
        mid=x["mid"]
        ra=x["ra"].split(",")
        territory=[]
        for r in ra:
            territory.append(ra_arr[int(r)-1])
        results_data['CountryTerritory'].append(",".join(territory))
        region=country_data.get(ra[0])[mid]
        results_data['Region'].append(region)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for index,link in enumerate(results_data["Url"]):
            url=link
            country=results_data["CountryTerritory"][index]
            region=results_data["Region"][index]
            task = asyncio.ensure_future(fetch_data(session, url,country,region))
            tasks.append(task)

        results = await asyncio.gather(*tasks)

    df=pd.DataFrame(results)
    df['References_key'] = df['References'].str.rsplit(',', n=2).str[-2]
    df=df.drop_duplicates(subset=['Url'])
    from datetime import datetime
    now=datetime.now()
    now=now.strftime('%Y-%m-%d %H%M%S')
    csv_name=f"SWIC_CAPWarnings_asynco_{now}.csv"
    csv_path=f"{csv_name}"
    df.to_csv(csv_path,index=False)
    # Example usage
    storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=freestoragecheck;AccountKey=sCeTnlqxzIYu5XnjoUsZKQJDsTs5pEXMetwS6xl/TGVtX/F64b/Ps3GbBMw1ylNSXVO1pFYz3s2e+AStHip3VQ==;EndpointSuffix=core.windows.net"
    container_name = "demo-data"
    file_path = csv_path
    blob_name = csv_name

    upload_csv_to_blob(storage_connection_string, container_name, file_path, blob_name)
    os.remove(csv_path)

