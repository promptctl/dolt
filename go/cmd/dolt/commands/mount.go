// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

type mountOpts struct{}

var mountDocs = cli.CommandDocumentationContent{
	ShortDesc: `Mount Dolt as a file system`,
	LongDesc:  `Mount Dolt as a file system`,
	Synopsis: []string{
		`[{{.LessThan}}mountpoint{{.GreaterThan}}]`,
	},
}

type MountCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MountCmd) Name() string {
	return "mount"
}

// Description returns a description of the command
func (cmd MountCmd) Description() string {
	return `Mount Dolt as a file system`
}

func (cmd MountCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(showDocs, ap)
}

func (cmd MountCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	return ap
}

func (cmd MountCmd) RequiresRepo() bool {
	return true
}

func parseMountArgs(apr *argparser.ArgParseResults) (*mountOpts, error) {
	return &mountOpts{}, nil
}

func (cmd MountCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, mountDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	_, err := parseMountArgs(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	mountpoint := apr.Arg(0)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("dolt"),
		fuse.Subtype("doltfs"),
	)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	defer c.Close()

	err = fs.Serve(c, FS{db: dEnv.DoltDB})
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	return 0
}

// Different handlers for different message types, which specify different paths.
// Examples:
// / is a StoreRoot
// /workingSets/heads/main/ is a WorkingSet
// /workingSets/heads/main/working is a RootValue
// /refs/heads/main/ is a rootvalue
// /addresses/rt9gl00583v5ulof6qkhun355q6kcpbq is whatever the address resolves to.
// /working, /staged, and /head give you currently checked-out branch

// FS implements the hello world file system.
type FS struct {
	db *doltdb.DoltDB
}

func (f FS) Root() (fs.Node, error) {
	return AddressesDir{
		db: f.db,
	}, nil
}

type Directory interface {
	fs.Node
	fs.NodeStringLookuper
}

type ListableDirectory interface {
	Directory
	fs.HandleReadDirAller
}

type File interface {
	fs.Node
	fs.HandleReadAller
}

type AddressesDir struct {
	db *doltdb.DoltDB
}

var _ Directory = AddressesDir{}

func (AddressesDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0o555
	return nil
}

func (d AddressesDir) Lookup(ctx context.Context, specRef string) (fs.Node, error) {
	// if it's a correct hash, read the node as a blob.
	if !hashRegex.MatchString(specRef) {
		return nil, syscall.ENOENT
	}
	refHash, err := parseHashString(specRef)
	if err != nil {
		return nil, err
	}
	value, err := d.db.ValueReadWriter().ReadValue(ctx, refHash)
	if err != nil {
		return nil, err
	}

	switch v := value.(type) {
	case types.SerialMessage:
		node, fileId, err := tree.NodeFromBytes(v)
		switch fileId {
		case serial.BlobFileID:
			return &Blob{
				ns:   d.db.NodeStore(),
				node: node,
			}, err
		}
	}
	return nil, syscall.ENOENT
}

/*
var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: "hello", Type: fuse.DT_File},
}

func (Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return dirDirs, nil
}*/

type Blob struct {
	ns   tree.NodeStore
	node tree.Node
}

var _ File = Blob{}

func (b Blob) Attr(ctx context.Context, a *fuse.Attr) (err error) {
	a.Mode = 0o444
	// TODO: Report size just from the tree
	size := uint64(0)
	err = tree.WalkNodes(ctx, b.node, b.ns, func(ctx context.Context, n tree.Node) error {
		if n.IsLeaf() {
			size += uint64(len(n.GetValue(0)))
		}
		return nil
	})
	a.Size = size
	return err
}

func (b Blob) ReadAll(ctx context.Context) (result []byte, err error) {
	err = tree.WalkNodes(ctx, b.node, b.ns, func(ctx context.Context, n tree.Node) error {
		if n.IsLeaf() {
			result = append(result, n.GetValue(0)...)
		}
		return nil
	})
	return result, err
}
