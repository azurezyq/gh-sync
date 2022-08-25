#!/usr/bin/env python3
import json
import argparse
from datetime import datetime
from github import Github
import os
import re
from retry import retry
import time


@retry(tries=3, delay=2)
def GetRepos(g, owner):
  result = []
  for repo in g.get_organization(owner).get_repos():
    result.append(repo)
  return result

@retry(tries=3, delay=2)
def GetReviews(pr):
  rs = []
  for r in pr.get_reviews():
    rs.append({
      'user' : r.user.login,
      'state' : r.state,
      'submittedAt' : ToISO(r.submitted_at),
      })
  return rs


@retry(tries=3, delay=2)
def ToObject(pr):
  now = datetime.utcnow()
  return {
      'id' : pr.id,
      'recordTimestamp' : ToISO(now),
      'additions' : pr.additions,
      'deletions' : pr.deletions,
      'author' : pr.user.login,
      'state' : pr.state,
      'createdAt' : ToISO(pr.created_at),
      'updatedAt' : ToISO(pr.updated_at),
      'closedAt' : ToISO(pr.closed_at),
      'title' : pr.title,
      'url' : ConvertUrl(pr.url),
      'body' : pr.body,
      'reviews' : GetReviews(pr),
      }

def RateLimit(config):
  remaining, _ = config['client'].rate_limiting
  print('rate limiting remaining', remaining)
  while remaining < 200:
    time.sleep(60)
    remaining = config['client'].rate_limiting
    print('rate limiting remaining', remaining)



@retry(tries=3, delay=2)
def GetPullRequests(config, repo):
  result = []
  i = 0
  for pr in repo.get_pulls(state='all'):
    print(pr.number)
    RateLimit(config)
    time.sleep(config['sleep_between_gets'])
    if pr.user.login in config['exclude_users']:
      continue
    result.append(ToObject(pr))
  return result

def ToISO(d):
  if not d:
    return None
  return d.isoformat() + 'Z'

def ConvertUrl(url):
  '''https://api.github.com/repos/tidbcloud/infra-api/pulls/860'''
  return re.sub('https://api.github.com/repos/([^/]+)/([^/]+)/pulls/([0-9]+)',
      r'https://github.com/\1/\2/pull/\3',
      url)

if __name__ == '__main__':
  #from github import enable_console_debug_logging
  #enable_console_debug_logging()
  parser = argparse.ArgumentParser(description='github PR sync')
  parser.add_argument('--owners', type=str, nargs='?', default='tidbcloud')
  parser.add_argument('--out', type=str, nargs='?', default='/tmp/result.jsonl')
  parser.add_argument('--sleep_between_gets', type=int, nargs='?', default=0)
  parser.add_argument('--exclude_users', type=str, nargs='?', default='tidbcloud-bot,github-actions,ti-srebot,ti-chi-bot,dependabot')
  args = parser.parse_args()
  g = Github(os.environ['GITHUB_TOKEN'])
  config = {
      'exclude_users' : args.exclude_users.strip().split(','),
      'sleep_between_gets' : args.sleep_between_gets,
      'client' : g,
      }
  result = []
  for owner in args.owners.strip().split(','):
    repos = GetRepos(g, owner)
    for repo in repos:
      print(owner, repo.name)
      prs = GetPullRequests(config, repo)
      for pr in prs:
        pr['repo'] = repo.name
        pr['owner'] = owner
      result.extend(prs)
    break
  with open(args.out, 'w') as fp:
    for x in result:
      json.dump(x, fp)
      fp.write('\n')

