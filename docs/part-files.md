# qBittorrent `.parts` files and partially selected torrents

qBittorrent/libtorrent can keep bytes needed to hash-check pieces overlapping
skipped files in a hidden `.<infohash>.parts` file in the torrent **save directory**.
The file is not part of the user's selected content, but some selected pieces
cannot verify without it. The torrent's aggregate progress may remain below
100% even when every selected file has progress 100% and `amount_left` is zero.

SeedCache backs up an existing local `.parts` file alongside the selected
payload before marking a new replication as verified. It uses the same path
mappings, collision checks and rsync checksum verification as selected files.
A backup conflict is never overwritten automatically. The part-file is **not**
added to the selected-file manifest or counted toward its selected payload size.

On restoration, SeedCache preserves an existing local `.parts` file and, if
missing, downloads the remote file to a **separate staging directory** named
`.seedcache-staged-partfiles` beneath the torrent save directory. It never
writes to libtorrent's live `.parts` path while qBittorrent is running.
Restoration pauses with an instruction to **stop qBittorrent**, move the staged
file to the torrent save directory (removing the staging directory component),
**start qBittorrent**, and retry `seedcache restore`. This one-time offline
installation allows libtorrent to load the original piece mapping safely.
SeedCache intentionally does not restart a shared qBittorrent process because
that disrupts unrelated torrents.

Eviction removes only the selected payload listed in the manifest. Local
`.parts` files remain on the cache disk, allowing future restorations without
a qBittorrent restart in the normal case. For previously adopted torrents
whose only `.parts` copy is on the remote, expect the one-time staging flow.

The qBittorrent process must be able to read/write staged files. SeedCache
sets their group to match the restored selected payload and sets mode `0660`.
Its container user must be a member of that shared group to change the group.

Do not manually delete `.parts` files or change skipped-file priorities to
make aggregate torrent progress reach 100%. Check the selected files and the
recheck outcome instead.
