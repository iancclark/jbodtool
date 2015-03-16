CREATE TABLE enclosures (dev,sas unique,name, model);
CREATE TABLE disks (dev,sas unique,label,phy, enclosure, slot, model, diskid, online, badblocks);
