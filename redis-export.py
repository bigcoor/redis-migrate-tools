#!/usr/bin/env python
import sys
import redis
import argparse
import urlparse
import time
import os
import curses
import signal
import json

def fail(msg):
  print >> sys.stderr, msg
  exit(1)

def redisHost(r):
  return r.connection_pool.connection_kwargs['host']

def redisPort(r):
  return r.connection_pool.connection_kwargs['port']

def redisPassword(r):
  return r.connection_pool.connection_kwargs['password']

def writeLn(y, x, txt, attr=0):
    stdscr.move(y,0)
    stdscr.clrtoeol()
    stdscr.move(y,x)
    stdscr.addstr(txt, attr)
    stdscr.refresh()
 
def signalWinch(signum, frame):
  pass
    
def valOrNA(x):
  return x if x != None else 'N/A'

def bytesToStr(bytes):
  if bytes < 1024:
    return '%dB'%bytes
  if bytes < 1024*1024:
    return '%dKB'%(bytes/1024)
  if bytes < 1024*1024*1024:
    return '%dMB'%(bytes/(1024*1024))
  return '%dGB'%(bytes/(1024*1024*1024))



def getRedisList(url):
  url = urlparse.urlparse(url)
  if not url.scheme:
    url = 'redis://' + url
    url = urlparse.urlparse(url)
  if url.scheme != 'redis':
    fail('Invalid scheme %s for %s, aborting'%(url.scheme, url))
  r = redis.Redis(host=url.hostname, port=(url.port if url.port else 6379), password=url.password)
  try:
    ver = r.info()['redis_version']
    r.ver = ver
  except redis.ConnectionError as e:
    fail('Failed connecting (%s) to %s, aborting'%(e, url))
  
  return r

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Interactively migrate a bunch of redis servers to another bunch of redis servers.')
  parser.add_argument('--src', metavar='src_url', nargs='+', required=True, help='list of source redises to sync from')
  #parser.add_argument('--dst', metavar='dst_url', nargs='+', required=True, help='list of destination redises to sync to')
  
  args = parser.parse_args()
  
  # if len(args.src) != len(args.dst):
  #     fail('Number of sources must match number of destinations')

  r = getRedisList(args.src[0])
  # dsts = getRedisList(args.dst)

  stdscr = curses.initscr()
  curses.halfdelay(10)
  curses.noecho()
  curses.curs_set(0)
  
  signal.signal(signal.SIGWINCH, signalWinch)
  
  try:
    # Get aggregate sizes from sources
    #
    
    # Read data from redis and wirte to file.
    with open('redis.json', 'w+') as dataFile:
    	out = ''
    	for key in r.scan_iter():
    		doc = {}
    		type = r.type(key)
    		expire = r.ttl(key)

    		if type == r.REDIS_STRING:
    			doc['expire'] = expire
    			doc['type'] = type
    			doc['key'] = key
    			doc['val'] = r.get(key)
    		elif type == r.REDIS_HASH:
    			doc['expire'] = expire
    			doc['type'] = type
    			doc['key'] = key
    			doc['val'] = r.hGetAll(key)
    		elif type == r.REDIS_LIST:
    			doc['expire'] = expire
    			doc['type'] = type
    			doc['key'] = key
    			doc['val'] = r.lRange(key, 0, -1)
    		elif type == r.REDIS_SET:
    			doc['expire'] = expire
    			doc['type'] = type
    			doc['key'] = key
    			doc['val'] = r.sMembers(key)
    		elif type == r.REDIS_ZSET:
    			doc['expire'] = expire
    			doc['type'] = type
    			doc['key'] = key
    			doc['val'] = r.zRange(key)
    		else:
    			pass
    		out += json.dumps(doc) + "\n"
    	dataFile.wirte(out)
  finally:
    curses.nocbreak()
    curses.echo()
    curses.curs_set(1)
    curses.endwin()
            
        
