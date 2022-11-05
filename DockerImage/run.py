import os
import datetime
import math
from time import sleep

import vk_api
import requests
from decouple import config

def intTryParse(value):
    try:
        return int(value), True
    except ValueError:
        return value, False

def GetExistingCardDirs(dir: str):
    result = set()
    for name in os.listdir(dir):
        _, parsed = intTryParse(name)
        if parsed:
            result.add(parsed)
    return result

def GetWall(domain:str, token:str, count:int=100, offset:int=0):
    vk_session = vk_api.VkApi(token=token)

    vk = vk_session.get_api()
    
    wall_contents = {}

    """
    wall.get — 5000 calls per day allowed
    offset — used to get posts that are below top 100
    """
    c = 0
    v = 0
    v_prev = None
    ads = []
    photo_ids = set()
    video_ids = set()
    post_ids = set()

    
    posts = vk.wall.get(domain=domain, count=count, filter=all, offset=offset)['items']
    try:
        for item in posts:
            duplicate = False
            c += 1
            if item['id'] in post_ids:
                print(f"Duplicate post {item['id']}\n")
                continue
            post_ids.add(item['id'])
            if item['marked_as_ads'] == 1:
                ads.append((item['id']))
            if 'attachments' in item:
                v += 1
                photos = set()
                post_id = item['id']
                date = item['date']
                text_content = item['text']

                for images in item['attachments']:
                    if images['type'] == 'photo':
                        # check if duplicate, but it is unreliable, because if post is reuploaded it gets new id 
                        # so catches only reposts
                        if images['photo']['id'] in photo_ids:
                            print(f"Duplicate photo {item['id']}")
                            duplicate = True
                            continue
                        photo_ids.add(images['photo']['id'])
                        try:
                            # photo sizes: https://vk.com/dev/photo_sizes
                            # we are intereseted in size 'x'
                            propSizePhoto = [x['url'] for x in images['photo']['sizes'] if x['size']=='x']
                            if len(propSizePhoto) == 0:
                                raise(f"Photo {images['photo']['id']} does not have 'x' size")
                            photos.add(propSizePhoto[0])
                        except IndexError:
                            pass

                    # working with video, gets a thumbnail of video file as image
                    elif images['type'] == 'video':
                        if images['video']['id'] in video_ids:
                            print(f"Duplicate video {item['id']}")
                            duplicate = True
                            continue
                        video_ids.add(images['video']['id'])
                        resolutions = []
                        for key, value in images['video'].items():
                            if key.startswith('photo_'):
                                resolutions.append(key.split('_')[1])
                        res = f'photo_{max(resolutions)}'
                        photos.add(images['video'][res])

                # checks if there were any photos in the post, because we are not interested in post with text only
                # also stops from adding exact duplicates
                if photos != set() and not duplicate:
                    wall_contents[post_id] = {
                        'date': date,
                        'owner_id': item['owner_id'],
                        'text': text_content,
                        'images': list(photos)
                    }
        return wall_contents
        
    except Exception as e:
        print(f'{e} occurred on post {c}\n')
        return

def InvokeClassifier(endpoint:str, text:str):
    requestData = [{'text': text}]

    headers = {
        "Content-Type":"application/json; format=pandas-records"
    }

    x = requests.post(endpoint, headers=headers, json = requestData)
    if not x.ok:
        raise f"Request to classifier failed. status code {x.status_code}: {x.text}"
    res = x.json
    return res[0]

def Main():
    cardsDir=config('CARDS_DIR', default=os.path.join(".","db"))

    vkToken=config('VK_TOKEN')
    vkGroupName=config('VK_GROUP_NAME')

    locationAddressText=config('LOCATION_ADDRESS')
    locationLat=config('LOCATION_LAT',cast=float)
    locationLon=config('LOCATION_LON',cast=float)

    lostFoundClassifierEndpoint=config('LOST_FOUND_CLASSIFIER_ENDPOINT')
    catDogClassifierEndpoint=config('CAT_DOG_CLASSIFIER_ENDPOINT')
    maleFemaleClassifierEndpoint=config('MALE_FEMALE_CLASSIFIER_ENDPOINT')

    numOfCrawlers=config('NUM_OF_CRAWLERS', default=1, cast=int)
    minPollingIntervalSec = config('MIN_POLL_INTERVAL', default=600, cast=int)
    targetApiRequestsCountPerDay = config('API_REQUESTS_PET_DAY', default=4000, cast=int) # 5000 calls per day allowed
    knownCardsTrackingCount=config('KNOWN_CARDS_TRACKING_COUNT', default=1024, cast=int)

    def ClassifySpeciesByText(text:str):
        classStr = InvokeClassifier(catDogClassifierEndpoint,text)
        # speciesTypesDesc = ["Cat","Dog","Other"]
        
        # re-encoding
        if classStr == "Cat":
            return "cat"
        elif classStr == "Dog":
            return "dog"
        elif classStr == "Other":
            return None
        raise f"Unexpected species classified: {classStr}"

    def ClassifyCardTypeByText(text:str):
        classStr = InvokeClassifier(lostFoundClassifierEndpoint,text)
        # messageTypesDesc = ["Lost","Found","NotRelevant/Other"]

        # re-encoding
        if classStr == "Lost":
            return "lost"
        elif classStr == "Found":
            return "found"
        elif classStr == "NotRelevant/Other":
            return None
        raise f"Unexpected lost/found card type classified: {classStr}"
        
    def ClassifySexByText(text:str):
        classStr = InvokeClassifier(lostFoundClassifierEndpoint,text)
        # sexesDesc = ["Female","Male","NotDescribed/Other"]

        # re-encoding
        if classStr == "Female":
            return "female"
        elif classStr == "Male":
            return "male"
        elif classStr == "NotDescribed/Other":
            return None
        raise f"Unexpected male/female animal sex classified: {classStr}"

    if not os.path.exists(cardsDir):
        os.makedirs(cardsDir)

    print(f"Cards dir:\t{cardsDir}\nCK Group Name:\t{vkGroupName}\nNumber of crawlers which uses the same API key:\t{numOfCrawlers}\nTargeting {targetApiRequestsCountPerDay} API requests per day (over all crawlers)")

    escapedGroupName = vkGroupName.replace("_",".")

    pollIntervalSec = max(minPollingIntervalSec, 24*60*60 / targetApiRequestsCountPerDay * numOfCrawlers)
    pollInterval:datetime.timedelta = datetime.timedelta(seconds=pollIntervalSec)

    print(f"Effective polling interval:\t{pollInterval}")

    knownCardsSet = GetExistingCardDirs(cardsDir)
    print(f"Found {len(knownCardsSet)} already downloaded cards")

    while True:
        startTime = datetime.datetime.now()
        
        # trancating too old cards
        if len(knownCardsSet) > knownCardsTrackingCount:
            print(f"Truncating {len(knownCardsSet)} known cards to {knownCardsTrackingCount}")
            knownCardsList = list(knownCardsSet)
            knownCardsList.sort(key=lambda x: x, reverse=True)
            knownCardsList = knownCardsList[:knownCardsTrackingCount]
            knownCardsSet = set(knownCardsList)

        posts = GetWall(vkGroupName, vkToken)
        newPostKeys = [key for key in posts if not(key in knownCardsSet)]
        print(f"Detected {len(newPostKeys)} new messages")

        for postID in posts:
            newPost = posts[postID]

            groupID = newPost['owner_id']

            postCreationUnixTime = newPost['date']
            postCreationTime = datetime.datetime.fromtimestamp(postCreationUnixTime)

            # ENCODE images

            text = newPost['text']
            card = {
                'uid': f"vk.{escapedGroupName}_{postID}",
                'animal': ClassifySpeciesByText(text),
                'location': {
                    'Address' : locationAddressText,
                    "Lat": locationLat,
                    "Lon": locationLon,
                    "CoordsProvenance": "Hardcoded in crawler configuration"
                },
                'event_time': postCreationTime.isoformat(),
                "event_time_provenance": "Время создание поста",
                "card_type": ClassifyCardTypeByText(text),
                "contact_info": {
                    "Comment": text,
                    "Tel":[],
                    "Website":[],
                    "Email":[],
                    "Name":""
                },
                "provenance_url": f"https://vk.com/wall{groupID}_{postID}"
            }

            sexDesc = ClassifySexByText(text)
            if not (sexDesc is None):
                card["animal_sex"] = sexDesc

            # composing JSON for pipeline

#             {
# "uid":"pet911ru_rf591749"
# "animal":"cat"
# "location":{
# "Address":"Московская область"
# "Lat":55.473808
# "Lon":38.163297
# "CoordsProvenance":"Указано на сайте pet911.ru"
# }
# "event_time":"2022-11-04T00:00:00Z"
# "event_time_provenance":"Указано на сайте pet911.ru"
# "card_type":"found"
# "contact_info":{
# "Comment":"Найден кот мальчик, прибился к СНТ &quot;муравушка&quot;, этим летом, коту примерно 5 лет очень ласковый немного пуглив и недоверчив с хорактером,но к людям подходит если позвать ,гладить даётся. Нужн ..."
# "Tel":[]
# "Website":[]
# "Email":[]
# "Name":"Евчик Елена Андреевна"
# }
# "images":[
# 0:{...
# }
# 1:{...
# }
# 2:{...
# }
# 3:{...
# }
# 4:{...
# }
# ]
# "provenance_url":"https://pet911.ru/%D0%9A%D1%80%D0%B0%D1%81%D0%BD%D0%BE%D0%B4%D0%B0%D1%80/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D0%BA%D0%BE%D1%88%D0%BA%D0%B0/rf591749"
# "animal_sex":"male"
# }

        finishTime = datetime.datetime.now()
        elapsed = finishTime - startTime
        toSleep = pollInterval - elapsed
        if toSleep > 0:
            print(f"Sleeping for {toSleep}")
            sleep(toSleep)


if __name__ == '__main__':
    Main()