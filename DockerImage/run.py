import base64
import os
import datetime
import math
import json
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

def GetWall(groupName:str, token:str, count:int=100, offset:int=0):
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

    posts = []
    fetchByDomain = False
    fetchByOwner = False
    ownerId = 0

    if groupName.startswith("club"):
        idCand = groupName[4:]
        id,success = intTryParse(idCand)
        if success:
            fetchByOwner = True
            ownerId = -id
        else:
            fetchByDomain = True
    else:
        fetchByDomain = True

    if fetchByDomain:
        print(f"Fetching by group human name: {groupName}")
        posts = vk.wall.get(domain=groupName, count=count, filter=all, offset=offset)['items']
    elif fetchByOwner:
        print(f"Fetching by owner_id: {ownerId}")
        posts = vk.wall.get(owner_id=ownerId, count=count, filter=all, offset=offset)['items']
    else:
        raise "Neither domain nor owner_id specified for fetch"
    
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
                    photo = images['photo']
                    if photo['id'] in photo_ids:
                        print(f"Duplicate photo {item['id']}")
                        duplicate = True
                        continue
                    photo_ids.add(images['photo']['id'])
                    
                    # photo sizes: https://vk.com/dev/photo_sizes
                    # we are intereseted in size 'x'
                    propSizePhoto = [x['url'] for x in photo['sizes'] if x['type']=='x']
                    if len(propSizePhoto) == 0:
                        raise(f"Photo {photo['id']} does not have 'x' size")
                    photos.add(propSizePhoto[0])
                    # except IndexError:
                    #     pass

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

def InvokeClassifier(endpoint:str, text:str):
    requestData = [{'text': text}]

    headers = {
        "Content-Type":"application/json; format=pandas-records"
    }

    x = requests.post(endpoint, headers=headers, json = requestData)
    if not x.ok:
        raise f"Request to classifier failed. status code {x.status_code}: {x.text}"
    res = x.json()
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

    pipelineNotificationURL = config('PIPELINE_NOTIFICATATION_URL',default='')

    numOfCrawlers=config('NUM_OF_CRAWLERS', default=1, cast=int)
    minPollingIntervalSec = config('MIN_POLL_INTERVAL_SEC', default=600, cast=int)
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
        classStr = InvokeClassifier(maleFemaleClassifierEndpoint,text)
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

    print(f"Cards dir:\t{cardsDir}\nVK Group Name:\t{vkGroupName}\nNumber of crawlers which uses the same API key:\t{numOfCrawlers}\nTargeting {targetApiRequestsCountPerDay} API requests per day (over all crawlers)")

    escapedGroupName = vkGroupName.replace("_","").replace(".","")

    pollIntervalSec = max(minPollingIntervalSec, 24*60*60 / targetApiRequestsCountPerDay * numOfCrawlers)
    pollInterval:datetime.timedelta = datetime.timedelta(seconds=pollIntervalSec)

    print(f"Effective polling interval:\t{pollInterval}")

    knownCardsSet = GetExistingCardDirs(cardsDir)
    print(f"Found {len(knownCardsSet)} already downloaded cards")

    def CheckCardOnDisk(cardId:int):
        return os.path.exists(os.path.join(cardsDir, f"{cardId}"))

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
        # print("Posts")
        # print(posts)
        newPostKeys = [key for key in posts if not((key in knownCardsSet) or CheckCardOnDisk(key))]
        print(f"Detected {len(newPostKeys)} new messages")

        for postID in newPostKeys:
            knownCardsSet.add(postID)
            newPost = posts[postID]

            groupID = newPost['owner_id']

            postCreationUnixTime = newPost['date']
            postCreationTime = datetime.datetime.fromtimestamp(postCreationUnixTime)


            # fetching images
            imagesBytes = []
            encodedImages = []
            for imageUrl in newPost['images']:
                res = requests.get(imageUrl)
                if not res.ok:
                    raise f"Failed to download photo for card {postID} from url {imageUrl}"
                imageBytes = res.content
                imagesBytes.append(imageBytes)

                contentMime = res.headers["content-type"]
                if not contentMime.startswith("image/"):
                    raise f"Downloaded image has not image mime: {contentMime}"
                imageMime = contentMime[len("image/"):]
                if imageMime == "jpeg":
                    imageMime = "jpg"

                # encoding images
                im = {
                    'type': imageMime,
                    'data': base64.encodebytes(imageBytes).decode("utf-8").replace("\n","")
                }
                encodedImages.append(im)


            # composing JSON for pipeline
            text = newPost['text']
            animal = ClassifySpeciesByText(text)
            if animal is None:
                print(f"Skipping card {postID} as species can't be determened")
                continue
            else:
                print(f"Post {postID} has been classified as discribing {animal}")
            card_type = ClassifyCardTypeByText(text)
            if animal is None:
                print(f"Skipping card {postID} as card type can't be determened")
                continue
            else:
                print(f"Post {postID} has been classified as discribing {card_type}")
            card = {
                'uid': f"vk-{escapedGroupName}_{postID}",
                'animal': animal,
                'location': {
                    'Address' : locationAddressText,
                    "Lat": locationLat,
                    "Lon": locationLon,
                    "CoordsProvenance": "Hardcoded in crawler configuration"
                },
                'event_time': postCreationTime.isoformat()+"Z",
                "event_time_provenance": "Время публикации поста",
                "card_type": card_type,
                "contact_info": {
                    "Comment": text,
                    "Tel":[],
                    "Website":[],
                    "Email":[],
                    "Name":""
                },
                "images": encodedImages,
                "provenance_url": f"https://vk.com/wall{groupID}_{postID}"
            }

            sexDesc = ClassifySexByText(text)
            print(f"Post {postID} has been classified as discribing pet to have the following sex: {sexDesc}")

            if not (sexDesc is None):
                card["animal_sex"] = sexDesc

            if pipelineNotificationURL == '':
                print(f"SKIPPING pipeline notification as pipeline notification URL is not defined")
            else:
                print(f"Notifying the pipeline to submit card {postID}")
                requests.post(pipelineNotificationURL, json=card)
                print(f"Successfully notified pipeline about card {postID}")

            # dumping to disk
            cardDir = os.path.join(cardsDir,f"{postID}")
            os.makedirs(cardDir)
            # writing images
            for i,im in enumerate(card['images']):
                fName = f"{i}.{im['type']}"
                with open(os.path.join(cardDir,fName),"wb") as f:
                    f.write(imagesBytes[i])
                print(f"wrote image {i} for card {postID}")
                im['type'] = "file"
                im['data'] = fName
            with open(os.path.join(cardDir,"card.json"),"w") as f:
                json.dump(card, f)
            print(f"Wrote card json for card {postID}")

        

        finishTime = datetime.datetime.now()
        elapsed = finishTime - startTime
        toSleep = pollInterval - elapsed
        if toSleep.total_seconds() > 0:
            print(f"Sleeping for {toSleep}")
            sleep(toSleep.total_seconds())


if __name__ == '__main__':
    Main()