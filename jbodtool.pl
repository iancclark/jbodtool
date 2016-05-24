#!/usr/bin/perl -w

use DBI;
use Getopt::Long;
use Pod::Usage;
use strict;

=head1 NAME

whereis.pl - A utility to tie together camcontrol, sg_ses and geom labels

=cut

my $dbh=DBI->connect("dbi:SQLite:dbname=/var/db/jbodtool.sqlite","","",{AutoCommit=>0,RaiseError=>1});
my $time=30;
my $renenc;

GetOptions(
        'usage'=>sub {pod2usage(1);exit},
        'help'=>sub {pod2usage(-verbose=>2);exit},
        'enclosure=s'=>\$renenc,
        'scan'=>sub{&scan()},
	'identify=s'=>sub{&identify($_[1])},
	'time=i'=>\$time,
	'spindown=s'=>sub{&spindown($_[1])},
        'name=s'=>sub{ if(defined($renenc)) { &name_enc($renenc,$_[1]) } else { die("I need an enclosure to name")} },
        'list'=>sub { &list() },
        'finddev=s'=>sub {&finddev($_[1])},
        'list-enc'=>sub { &list_enc() },
) or die("Use -usage for usage");

=head1 SYNOPSIS

whereis.pl [options]

Options:
 --scan
 --list
 --time=<seconds>
 --identify=<block device> | <sas address> | <geom label> | <disk id>
 --spindown=<block device> | <sas address> | <geom label> | <disk id> | unused
 --finddev=<block device> | <sas address> | <geom label> | <disk id>
 --enclosure=<block device> | <sas address>
 --name="friendly name for an enclosure"

=cut

sub scan {

  # Lets cache some info
  my $zpools=&get_zpool_status();
  my $glabels=&get_glabels();
  my $gstripes=&get_gstripes();

  my $sthdisk=$dbh->prepare("INSERT OR REPLACE INTO disks (dev,sas,enclosure,model,online,label,slot,pool,badblocks,diskid) VALUES (?,?,?,?,strftime('%s','now'),?,?,?,?,?)");
  my $sthenc=$dbh->prepare("INSERT OR REPLACE INTO enclosures (name, dev, sas,model) VALUES ((SELECT name FROM enclosures WHERE sas=?),?,?,?)");
  foreach my $enc (glob "/dev/ses*") {
    $enc=~s/\/dev\///;
    open(CAMCONTROL,"/sbin/camcontrol smpphylist /dev/$enc |");
    while(<CAMCONTROL>) {
      if(/\s+(\d+)\s+0x([0-9a-f]{16})\s+<([^>]+)>\s+\((\S+)\)/) {
        my $phy=$1;
        my $sas=$2; # NB: This is the SAS address of the port on the disk, not its LU.
        my $model=$3;
        my @devs=split(',',$4);
        if($#devs!=1) { # expect a pass-thru device plus one other
          # Something awry.
          print "Strange number of device entries:\n>  $_";
        }

        foreach my $dev (grep(/ses\d/,@devs)) {
          # enclosure
          $sthenc->execute($sas,$dev,$sas,$model);
        }
        foreach my $dev (grep(/da\d/,@devs)) {
          # disk
          my $slot=&get_slot($sas,$enc);
          my $label;
          my $pool="";
          my ($diskid,$badblocks)=&get_smart($dev);
          my $oldbadblocks=$dbh->selectrow_array("SELECT badblocks FROM disks WHERE sas=?",undef,$sas);
          if(defined($glabels->{$dev})) {
            $label=$glabels->{$dev};
          } elsif(defined($gstripes->{$dev})) {
            $label=$gstripes->{$dev};
          }
          # Check dev against pool
          if(defined($zpools->{$dev}) && defined($zpools->{$dev}->{pool})) {
            $pool=$zpools->{$dev}->{pool};
          } 
          # Check label against pool
          if(defined($label)) {
            if(defined($zpools->{"label/".$label}) && defined($zpools->{"label/".$label}->{pool})) {
              $pool=$zpools->{"label/".$label}->{pool};
            } elsif(defined($zpools->{"stripe/".$label}) && defined($zpools->{"stripe/".$label}->{pool})) {
              $pool=$zpools->{"stripe/".$label}->{pool};
            }
          } 
          
          if(!defined($oldbadblocks)) {
            print "New disk $dev ($sas) at $enc $slot\n";
          } elsif($badblocks>$oldbadblocks) {
            print "Badblocks have increased from $oldbadblocks to $badblocks on $dev";
            if(defined($label)) {
              print " label: $label";
            }
            if($pool ne "") {
              print " pool: $pool";
            }
            print "\n";
          } elsif($badblocks<$oldbadblocks) {
            #print "Badblocks got better? Or a bug?\n";
            $badblocks=$oldbadblocks;
          }
          $sthdisk->execute($dev,$sas,$enc,$model,$label,$slot,$pool,$badblocks,$diskid);
        } 
      }
    }
    close(CAMCONTROL);
    if($? != 0) {
      print(STDERR "camcontrol returned $?\n");
    }
  }
  $dbh->commit;
  my $sth=$dbh->prepare("SELECT strftime('%Y-%m-%dT%H:%M:SZ',disks.online,'unixepoch') as since, disks.dev AS dev,disks.diskid AS diskid,COALESCE(disks.label,'') AS label, disks.sas AS sas, enclosures.name AS enclosure,disks.slot AS slot FROM disks,enclosures WHERE strftime('%s','now','-15 minutes')>disks.online and disks.dev!='gone' and enclosures.dev=disks.enclosure") || die $?;
  $sth->execute();
  while(my $r=$sth->fetchrow_hashref) {
    print "Lost $r->{dev} s/n: $r->{diskid} label: $r->{label} wwn: $r->{sas} from $r->{enclosure} $r->{slot} since $r->{since}\n";
    $dbh->do("UPDATE disks SET dev='gone' WHERE dev=? AND diskid=?",undef,$r->{dev},$r->{diskid});
  }
  $dbh->commit;
}

sub identify {
  my($device)=@_;
  my ($slot,$enclosure) = $dbh->selectrow_array("SELECT slot,enclosure FROM disks WHERE dev=? OR sas=? OR label=? OR diskid=? LIMIT 1",undef,$device,$device,$device,$device);
  if(defined($slot) && defined($enclosure)) {
    system("/usr/local/bin/sg_ses --descriptor='$slot' --set=ident /dev/$enclosure");
    if($time!=0) {
      print("Identifying $device ($enclosure:$slot) for $time seconds!\n");
      sleep($time);
      system("/usr/local/bin/sg_ses --descriptor='$slot' --clear=ident /dev/$enclosure");
    } else {
      print("Identifying $device ($enclosure:$slot) indefinitely, run again to clear\n");
    }
  } else {
    print "Don't know about device $device\n";
  }
}

sub spindown {
  my($device)=@_;
  if($device eq "unused") { 
    my $sth=$dbh->prepare("SELECT dev FROM disks WHERE pool='' and online>strftime('%s','now','-1 hour')");
    $sth->execute;
    while(my $r=$sth->fetchrow_hashref) {
      print("About to stop: ".$r->{dev}."\n");
      &spindown($r->{dev});
    }
    return;
  }
  my($dev,$model)=$dbh->selectrow_array("SELECT dev,model FROM disks WHERE dev=? OR sas=? OR label=? OR diskid=?",undef,$device,$device,$device,$device);
  if($model=~/^ATA/) {
    print "Attempt ATA standby on $device ($dev)\n";
    system("camcontrol standby $dev");
  } else {
    print "Attempt SCSI stop on $device ($dev)\n";
    system("camcontrol stop $dev");
  }
}

sub list {
  $~="LIST";
  my $row;
  $row={dev=>"Device",dsas=>"Dev SAS PortAddr",label=>"Label",diskid=>"Disk ID",esas=>"Enc SAS Addr",name=>"Enc",slot=>"Slot",online=>"",bad=>"Bad",pool=>"Pool"};
  format LIST =
@<<<<< @<<<<<<<<<<<<<<< @<<<<<< @<<<<<<<< @<<<<<<< @<< @<<< @<<<
$row->{dev}, $row->{dsas}, $row->{label}, $row->{pool}, $row->{diskid}, $row->{name}, ($row->{slot} =~ s/Slot //r), $row->{bad}
.

  write;
  my $sth=$dbh->prepare("SELECT disks.dev as dev, COALESCE(disks.diskid,'-undef-') as diskid, disks.sas as dsas, COALESCE(disks.label,'-undef-') as label, enclosures.sas as esas, COALESCE(enclosures.name,enclosures.model) as name, disks.slot as slot, disks.online as online, badblocks AS bad, disks.pool FROM disks,enclosures WHERE disks.enclosure=enclosures.dev");
  $sth->execute;
  while($row=$sth->fetchrow_hashref) {
    write;
  } 
}

sub list_enc {
  $~="LISTENC";
  my $row;
  $row={dev=>"Device",sas=>"SAS Address",name=>"Name",model=>"Model"};
  format LISTENC =
@<<<<< @<<<<<<<<<<<<<<< @<<< @<<<<<<<<<<<<<<<<<<<<<<
$row->{dev},$row->{sas},$row->{name},$row->{model}
.
  write;
  my $sth=$dbh->prepare("SELECT dev,sas,name,model FROM enclosures");
  $sth->execute;
  while($row=$sth->fetchrow_hashref) {
    write;
  }
}

sub finddev {
  my ($dev)=@_;
  print $dbh->selectrow_array("SELECT dev FROM disks WHERE label=? OR sas=? OR diskid=?",undef,$dev,$dev,$dev);
}

sub name_enc {
  my ($enclosure,$name) = @_;
  print "Adding name $name to $enclosure\n";
  $dbh->do("UPDATE enclosures SET name=? WHERE sas=? or dev=?",undef,$name,$enclosure,$enclosure);
  $dbh->commit;
}

sub get_zpool_status {
  open(ZPOOL,"/sbin/zpool status |");
  my $cur_pool;
  my $zpools={};
  while(<ZPOOL>) {
    if(/\s+pool: (\S+)$/) {
      $cur_pool=$1;
    }

    #	    label/he6.006        ONLINE       0     0     0
    #	    replacing-7          ONLINE       0     0     0
    #	      label/st6.009      ONLINE       0     0     0
    #	      label/he6.007      ONLINE       0     0     0  (resilvering)

    if(/^\s{5,7}(\S+)\s+ONLINE/) {
      $$zpools{$1}={dev=>$1,pool=>$cur_pool,state=>"ONLINE"};
    }

    #            13015176704044211452  OFFLINE      0     0     0  was /dev/label/ark.001
    #            1080223557368156170  UNAVAIL      7    11     0  was /dev/label/st2.005
    if(/^\s{5,7}\S+\s+(\S+)\s+\d+\s+\d+\s+\d+\s+was \/dev\/(\S+)/) {
      $$zpools{$2}={dev=>$2,pool=>$cur_pool,state=>$1};
      print "Warning: disk $2 in zpool $cur_pool is $1\n";
    }
  }
  return $zpools;
}

sub get_glabels {
  open(GLABEL,"/sbin/glabel status|");
  my $glabels={};
  while(<GLABEL>) {
    if(/Components/) {
      next;
    }
    if(/label\/(\S+)\s+N\/A\s+(\S+)/) {
      $glabels->{$2}=$1;
    }
  }
  return $glabels;
}

sub get_gstripes {
  open(GSTRIPE,"/sbin/gstripe status -s|");
  my $gstripes={};
  while (<GSTRIPE>) {
    if(/stripe\/(\S+)\s+UP\s+(\S+)/) {
      $gstripes->{$2}=$1;
    }
  }
  return $gstripes;
}

sub get_slot {
  my ($sas,$enc)=(@_);
  # Search by SAS id
  open(SES,"/usr/local/bin/sg_ses -A $sas /dev/$enc|");
  while(<SES>) {
    if(/^(.*\S)\s+\[\d+,\d+\]\s+Element type: Array device slot$/){
      close(SES);
      return($1);
    }
  }
  close(SES);
  return(undef);
}

sub get_smart {
  my ($dev)=(@_);
  my $badblocks=0;
  my $diskid;
  open(SMART,"/usr/local/sbin/smartctl -Ai /dev/$dev |");
  while(<SMART>) {
    if(/Serial number:\s+(\S+)/i){
      $diskid=$1;
    }
    if(/Elements in grown defect list: (\d+)/) {
      $badblocks+=$1;
    }
    if(/Reallocated_Sector_Ct.+\s(\d+)\s*$/) {
      $badblocks+=$1;
    }
    if(/Current_Pending_Sector.+\s(\d+)\s*$/) {
      $badblocks+=$1;
    }
  }
  close(SMART);
  return($diskid,$badblocks);
}
