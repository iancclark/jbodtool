#!/usr/bin/perl -w

use DBI;
use Getopt::Long;
use Pod::Usage;

=head1 NAME

whereis.pl - A utility to tie together camcontrol, sg_ses and geom labels

=cut

$dbh=DBI->connect("dbi:SQLite:dbname=/var/db/jbodtool.sqlite","","",{AutoCommit=>0,RaiseError=>1});

my $time=30;
GetOptions(
        'usage'=>sub {pod2usage(1);exit},
        'help'=>sub {pod2usage(-verbose=>2);exit},
        'enclosure=s'=>\$enclosure,
        'scan'=>sub{&scan()},
	'identify=s'=>sub{&identify($_[1])},
	'time=i'=>\$time,
	'spindown=s'=>sub{&spindown($_[1])},
        'name=s'=>sub{ if(defined($enclosure)) { &name_enc($enclosure,$_[1]) } else { die("I need an enclosure to name")} },
        'list'=>sub { &list() },
        'finddev=s'=>sub {&finddev($_[1])},
) or die("Use -usage for usage");

=head1 SYNOPSIS

whereis.pl [options]

Options:
 --scan
 --list
 --time=<seconds>
 --identify=<block device> | <sas address> | <geom label> | <disk id>
 --spindown=<block device> | <sas address> | <geom label> | <disk id>
 --finddev=<block device> | <sas address> | <geom label> | <disk id>
 --enclosure=<block device> | <sas address>
 --name="friendly name for an enclosure"

=cut

sub scan {
  $dbh->do("UPDATE disks SET online='?'");
  my $sthdisk=$dbh->prepare("INSERT OR REPLACE INTO disks (dev,sas,phy,enclosure,model,online,badblocks) VALUES (?,?,?,?,?,'',(SELECT badblocks FROM disks WHERE sas=?))");
  my $sthenc=$dbh->prepare("INSERT OR REPLACE INTO enclosures (name, dev, sas,model) VALUES ((SELECT name FROM enclosures WHERE sas=?),?,?,?)");
  foreach $enc (glob "/dev/ses*") {
    $enc=~s/\/dev\///;
    print "Looking at enc $enc...".$dbh->selectrow_array("SELECT name FROM enclosures WHERE dev=?",undef,$enc)."\n";
    open(CAMCONTROL,"/sbin/camcontrol smpphylist /dev/$enc |");
    while(<CAMCONTROL>) {
      if(/\s+(\d+)\s+0x([0-9a-f]{16})\s+<([^>]+)>\s+\((\S+)\)/) {
        my $phy=$1;
        my $sas=$2;
        my $model=$3;
        my $dev=$4;
        my $diskid;
        my $slot;
        $dev=~s/pass\d+//;
        $dev=~s/,//;

        if($dev=~/ses\d+/) {
          # enclosure
          $sthenc->execute($sas,$dev,$sas,$model);
        } else {
          # disk, probably
          $sthdisk->execute($dev,$sas,$phy,$enc,$model,$sas);
          # Search by SAS id
          open(SES,"sg_ses -A $sas /dev/$enc|");
          while(<SES>) {
            if(/^(.*\S)\s+\[\d+,\d+\]\s+Element type: Array device slot$/){
              $slot=$1;
              $dbh->do("UPDATE disks SET slot=? WHERE sas=?",undef,$slot,$sas);
            }
          }
          close(SES);
          open(SMART,"smartctl -Ai /dev/$dev |");
          while(<SMART>) {
            if(/Serial number:\s+(\S+)/i){
              $diskid=$1;
              $dbh->do("UPDATE disks SET diskid=? WHERE sas=?",undef,$diskid,$sas);
            }
            if(/Elements in grown defect list: (\d+)/) {
              if($1>$dbh->selectrow_array("SELECT badblocks FROM disks WHERE sas=?",undef,$sas)) {
                print "$dev in $slot: $1 badblocks\n";
                $dbh->do("UPDATE disks SET badblocks=? WHERE sas=?",undef,$1,$sas);
              }
            }
            if(/Reallocated_Sector_Ct.+(\d+)\s*$/) {
              if($1>$dbh->selectrow_array("SELECT badblocks FROM disks WHERE sas=?",undef,$sas)) {
                print "$dev in $slot: $1 badblocks\n";
                $dbh->do("UPDATE disks SET badblocks=? WHERE sas=?",undef,$1,$sas);
              }
            }
            if(/Current_Pending_Sector.+(\d+)\s*$/) {
              if($1>0) {
                print "$dev in $slot: $1 pending sectors\n";
              }
            }
          }
        }
  
        open(GLABEL,"/sbin/glabel status -s $dev 2> /dev/null|");
        my $label=<GLABEL>;
        close(GLABEL);
        if(defined($label) && $label=~/label\/(\S+)\s+/) {
          $dbh->do("UPDATE disks SET label=? WHERE dev=?",undef,$1,$dev);
        } 
      }
    }
    close(CAMCONTROL);
  }
  $dbh->commit;
}

sub identify {
  my($device)=@_;
  my ($slot,$enclosure) = $dbh->selectrow_array("SELECT slot,enclosure FROM disks WHERE dev=? OR sas=? OR label=? OR diskid=? LIMIT 1",undef,$device,$device,$device,$device);
  if(defined($slot) && defined($enclosure)) {
    system("sg_ses --descriptor='$slot' --set=ident /dev/$enclosure");
    if($time!=0) {
      print("Identifying $device ($enclosure:$slot) for $time seconds!\n");
      sleep($time);
      system("sg_ses --descriptor='$slot' --clear=ident /dev/$enclosure");
    } else {
      print("Identifying $device ($enclosure:$slot) indefinitely, run again to clear\n");
    }
  } else {
    print "Don't know about device $device\n";
  }
}

sub spindown {
  my($device)=@_;
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
  my $row;
  $row={dev=>"Device",dsas=>"Dev SAS Addr",label=>"Label",diskid=>"Disk ID",esas=>"Enc SAS Addr",name=>"Enc",slot=>"Slot",online=>"",bad=>""};
  format STDOUT =
@<<<<< @<<<<<<<<<<<<<<< @<<<<<<<<<<<<<<<<<<<<<<<< @<<<<<<<<<<<<<<< @<< @<<<<<<
$row->{dev}.$row->{bad}, $row->{dsas}, $row->{label}, $row->{diskid}, $row->{name}, $row->{online}.$row->{slot}
.

  write;
  my $sth=$dbh->prepare("SELECT disks.dev as dev, disks.diskid as diskid, disks.sas as dsas, COALESCE(disks.label,disks.model) as label, enclosures.sas as esas, COALESCE(enclosures.name,enclosures.model) as name, disks.slot as slot, disks.online as online, CASE WHEN badblocks >0 THEN '*' ELSE '' END AS bad FROM disks,enclosures WHERE disks.enclosure=enclosures.dev");
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
