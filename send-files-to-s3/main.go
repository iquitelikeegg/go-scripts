package main

import (
	"archive/zip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	sourceDirPath := "."

	if len(os.Args) >= 2 {
		sourceDirPath = os.Args[1]
	}

	dateDirectories, err := ioutil.ReadDir(sourceDirPath)

	if err != nil {
		log.Fatal(err)
	}

	for _, dateDirectory := range dateDirectories {
		fmt.Println(dateDirectory.Name())
		resolvedPath := sourceDirPath + dateDirectory.Name()

		// Check if we are looking at a file or a directory
		isFile, err := os.Stat(resolvedPath)

		if err != nil {
			log.Fatal(err)
		}

		if isFile.Mode().IsRegular() {
			continue
		}

		fmt.Println("\nscanning: " + resolvedPath + "\n")
		files, err := ioutil.ReadDir(resolvedPath)

		if err != nil {
			log.Fatal(err)
		}

		var monthFiles []string

		for _, file := range files {
			monthFiles = append(monthFiles, resolvedPath+"/"+file.Name())
		}

		fmt.Println("compiled list of files:")
		fmt.Print(monthFiles)
		fmt.Print("\n\nzipping...\n")

		monthZip := fmt.Sprintf("%s.zip", dateDirectory.Name())

		if err := ZipFiles(monthZip, monthFiles); err != nil {
			log.Fatal(err)
		}

		fmt.Print(monthZip)

		break
	}
}

func ZipFiles(filename string, files []string) error {

	newZipFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer newZipFile.Close()

	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()

	// Add files to zip
	for _, file := range files {
		if err = AddFileToZip(zipWriter, file); err != nil {
			return err
		}
	}
	return nil
}

func AddFileToZip(zipWriter *zip.Writer, filename string) error {

	fileToZip, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	// Get the file information
	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	header.Name = filename

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}
