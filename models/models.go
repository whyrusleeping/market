package models

import (
	"sync"
	"time"

	"gorm.io/gorm"
)

type Repo struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time
	Did       string `gorm:"uniqueIndex"`

	// cache fields
	Lk    sync.Mutex `gorm:"-"`
	Setup bool       `gorm:"-"`
}

type Post struct {
	ID        uint `gorm:"primarykey"`
	Created   time.Time
	Indexed   time.Time
	DeletedAt gorm.DeletedAt

	Author   uint   `gorm:"uniqueIndex:idx_post_rkeyauthor"`
	Rkey     string `gorm:"uniqueIndex:idx_post_rkeyauthor"`
	Raw      []byte
	NotFound bool
	Cid      string

	Reposting  uint
	ReplyTo    uint
	ReplyToUsr uint
	InThread   uint
}

type PostCounts struct {
	Post       uint `gorm:"uniqueindex"`
	Likes      int
	Replies    int
	Reposts    int
	Quotes     int
	ThreadSize int
}

type PostCountsTask struct {
	Post uint
	Op   string
	Val  int
}

type Follow struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Rkey    string `gorm:"uniqueIndex:idx_follow_rkeyauthor"`
	Author  uint   `gorm:"uniqueIndex:idx_follow_rkeyauthor"`
	Subject uint
}

type Block struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Rkey    string `gorm:"uniqueIndex:idx_blocks_rkeyauthor"`
	Author  uint   `gorm:"uniqueIndex:idx_blocks_rkeyauthor"`
	Subject uint
}

type Like struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Subject uint
}

type LikedReply struct {
	ID          uint      `gorm:"primarykey"`
	Created     time.Time `gorm:"index:,sort:desc"`
	Reply       uint
	ReplyAuthor uint
	Op          uint
}

type Repost struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_reposts_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_reposts_rkeyauthor"`
	Subject uint
}

type List struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_lists_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_lists_rkeyauthor"`
	Raw     []byte
}

type ListItem struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_listitems_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_listitems_rkeyauthor"`
	Subject uint
	List    uint
}

type ListBlock struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_listblocks_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_listblocks_rkeyauthor"`
	List    uint
}

type Profile struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Repo    uint
	Raw     []byte
	Rev     string
}

type FeedGenerator struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_feedgen_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_feedgen_rkeyauthor"`
	Raw     []byte
	Did     string
}

type ThreadGate struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_threadgate_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_threadgate_rkeyauthor"`
	Raw     []byte
	Post    uint
}

type Image struct {
	ID     uint `gorm:"primarykey"`
	Post   uint `gorm:"index"`
	Cid    string
	Did    string
	Mime   string
	Cached bool
	Failed bool
}

type PostGate struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Subject uint
	Raw     []byte
}

type StarterPack struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Raw     []byte
	List    uint `gorm:"index"`
}
