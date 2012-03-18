select (extract (epoch from starttime)) as ts, starttime, detectorid, volume, speed, occupancy, status, dqflags from loopdata
where  
	starttime between '2011-09-16 10:00:00-07' and '2011-09-16 12:00:00-07'
	and volume is not null
	and speed is not null
	and occupancy is not null
	and dqflags is not null