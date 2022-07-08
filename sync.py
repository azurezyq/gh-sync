#!/usr/bin/env python3
import json
import subprocess
import argparse
from datetime import datetime, timedelta


parser = argparse.ArgumentParser(description='Process some integers.')

thirty_days_ago = datetime.today() - timedelta(days=30)
START_TIME = thirty_days_ago.strftime('%Y-%m-%d')

def InvokeGH(cmd):
  print(cmd)
  output = subprocess.check_output(cmd, shell=True)
  return json.loads(output)


PR_COLUMNS = [
		'additions',
		'assignees',
		'author',
		'closed',
		'closedAt',
		'createdAt',
		'deletions',
		'isDraft',
		'labels',
		'mergedAt',
		'mergedBy',
		'number',
		'state',
		'title',
		'updatedAt',
		'url',]


def GetPullRequests(gh_bin, start_date, owner, repo):
  prs = InvokeGH('{} pr list -L 1000 -R {}/{} -s all -S "created:>{}" --json={}'.format(gh_bin, owner, repo, start_date, ','.join(PR_COLUMNS)))
  for pr in prs:
    pr['repo'] = repo
    pr['owner'] = owner
    pr['author'] = pr['author']['login']
    for k, v in pr.items():
      if isinstance(v, list) or isinstance(v, dict):
        pr[k] = str(v)
  return prs


def GetRepos(gh_bin, owner):
  repos = InvokeGH('{} repo list -L 1000 {} --json=name'.format(gh_bin, owner))
  return (x['name'] for x in repos)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='github PR sync')
  parser.add_argument('--owners', type=str, nargs='?', default='tidbcloud')
  parser.add_argument('--gh_bin', type=str, nargs='?', default='gh')
  parser.add_argument('--out', type=str, nargs='?', default='/tmp/result.json')
  args = parser.parse_args()
  result = []
  for owner in args.owners.strip().split(','):
    repos = list(GetRepos(args.gh_bin, owner))
    for repo in repos:
      print(owner, repo)
      result.extend(GetPullRequests(args.gh_bin, START_TIME, owner, repo))
  with open(args.out, 'w') as fp:
    for x in result:
      json.dump(x, fp)
      fp.write('\n')

