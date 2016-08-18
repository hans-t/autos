import argparse

import requests
import credentials


# (Short-lived) App access token: https://developers.facebook.com/docs/facebook-login/access-tokens#apptokens
# Long-lived token docs: https://developers.facebook.com/docs/facebook-login/access-tokens#extending

parser = argparse.ArgumentParser()
parser.add_argument('token')
parser.add_argument('--version', default='v2.5', help='Facebook API version: vX.Y. Default: %(default)s.')
parser.add_argument('--app-id', default=credentials.FACEBOOK_APP_ID,
                    help='Facebook Application ID. To find your application ID, '\
                         'go to https://developers.facebook.com/apps.')
parser.add_argument('--app-secret', default=credentials.FACEBOOK_APP_SECRET,
                    help='Facebook App Secret. To find your application secret, '\
                         'go to your application dashboard.')
args = parser.parse_args()


endpoint_url = "https://graph.facebook.com/{api_version}/"\
               "oauth/access_token".format(api_version=args.version.rstrip('. '))

params = {
    'grant_type': 'fb_exchange_token',
    'client_id': args.app_id.strip(),
    'client_secret': args.app_secret.strip(),
    'fb_exchange_token': args.token.strip(),
}

resp = requests.get(endpoint_url, params=params).json()
access_token = resp['access_token']

filename = 'access_token.txt'
open(filename, 'w').write(access_token)

print('Access token:\n\n{access_token}\n'.format(access_token=access_token))
print('Long-lived access token is saved to: ' + filename)

