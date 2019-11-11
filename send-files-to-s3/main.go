package main

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	S3Region         = "eu-west-2"
	S3Bucket         = "dfp-datalake-london"
	bucketBasePath   = "dfp/raw/crime/data.police.uk"
	defaultOutputDir = "~/Projects/go-scripts-output"
)

func main() {
	sourceDirPath := "."

	if len(os.Args) >= 2 {
		sourceDirPath = os.Args[1]
	}

	outputPath := defaultOutputDir

	// The 3rd output is the output directory
	if len(os.Args) >= 3 {
		outputPath = os.Args[2]
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

		YearDirName, fileName := GenerateFileName(dateDirectory.Name())
		outputFile := fmt.Sprintf("%s/%s/%s.zip", outputPath, YearDirName, fileName)

		if err := ZipFiles(outputFile, monthFiles); err != nil {
			log.Fatal(err)
		}

		log.Print(fmt.Sprintf("Uploading file to S3 %s", outputFile))

		UploadToS3(outputFile, fmt.Sprintf("%s/%s.zip", YearDirName, fileName))

		break
	}
}

func UploadToS3(fileDir string, uploadedFileName string) error {
	s, err := session.NewSession(&aws.Config{Region: aws.String(S3Region)})
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Open(fileDir)

	if err != nil {
		return err
	}

	defer file.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := file.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	file.Read(buffer)

	_, err = s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(S3Bucket),
		Key:                  aws.String(fmt.Sprintf("%s/%s", bucketBasePath, uploadedFileName)),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(buffer),
		ContentLength:        aws.Int64(size),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})

	return err
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

// GenerateFileName - Generate a File name from a directory of format YYYY/MM eg. 2019-09
func GenerateFileName(directoryName string) (string, string) {
	pieces := strings.Split("-", directoryName)
	return pieces[0], pieces[1]
}
