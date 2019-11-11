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
	"os/user"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// constants
const (
	S3Region         = "eu-west-2"
	S3Bucket         = "dfp-datalake-london"
	bucketBasePath   = "dfp/raw/crime/data.police.uk"
	defaultOutputDir = "/Projects/go-scripts-output/send-files-to-s3"
)

func main() {
	fmt.Print(`
	// Accepts a directory as the first argument - that contains a set of directories representing crime data named in the format YYYY-MM
	// zips the content of these directories and uploads them to S3 to the data lake bucket under crimes/data.police.uk
	// Creates the years as directories and the month files as zip files.
	`)

	sourceDirPath := "."

	if len(os.Args) >= 2 {
		sourceDirPath = os.Args[1]
	}

	usr, err := user.Current()

	outputPath := usr.HomeDir + defaultOutputDir

	// The 3rd output is the output directory
	if len(os.Args) >= 3 {
		outputPath = os.Args[2]
	}

	dateDirectories, err := ioutil.ReadDir(sourceDirPath)

	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	var zipFiles [][2]string

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

		// Read the files from the directory
		log.Print("\nscanning: " + resolvedPath + "\n")
		files, err := ioutil.ReadDir(resolvedPath)

		if err != nil {
			log.Fatal(err)
		}

		var monthFiles []string

		for _, file := range files {
			monthFiles = append(monthFiles, resolvedPath+"/"+file.Name())
		}

		log.Print("compiled list of files:")
		log.Print(monthFiles)
		log.Print(fmt.Sprintf("generating filenames from %s", dateDirectory.Name()))

		YearDirName, fileName := GenerateFileName(dateDirectory.Name())
		outputFile := fmt.Sprintf("%s/%s/%s.zip", outputPath, YearDirName, fileName)

		fmt.Print("\n\nzipping...\n")

		wg.Add(1)

		go func(outputFile string, monthFiles []string) {
			defer wg.Done()

			log.Print("zipping to file " + outputFile)
			if err := ZipFiles(outputFile, monthFiles); err != nil {
				log.Fatal(err)
			}
		}(outputFile, monthFiles)

		zipFiles = append(zipFiles, [2]string{outputFile, fmt.Sprintf("%s/%s.zip", YearDirName, fileName)})
	}

	wg.Wait()

	fmt.Print("sending to s3...")
	fmt.Print(zipFiles)

	for _, fileToUpload := range zipFiles {
		log.Print(fmt.Sprintf("Uploading file to S3 %s to bucket %s, directory %s - filename %s", fileToUpload, S3Bucket, bucketBasePath, fileToUpload[1]))

		if err := UploadToS3(fileToUpload[0], fileToUpload[1]); err != nil {
			log.Fatal(err)
		}
	}
}

// UploadToS3 - uploads files to the s3 bucket and directory specified by the constants
func UploadToS3(fileDir string, uploadedFileName string) error {
	// Get the session
	s, err := session.NewSession(&aws.Config{Region: aws.String(S3Region)})

	if err != nil {
		return err
	}

	// Open the file
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

// ZipFiles - Zips up the files provided and writes them to the filename provided
// Expects a "/" directory seperated string - and takes the second to last value as a parent directory - and attempts to create it
func ZipFiles(filename string, files []string) error {
	// Make the parent directory
	filenameParts := strings.Split(filename, "/")
	parentDir := filenameParts[len(filenameParts)-2]

	fmt.Println("creating directory if not exists " + parentDir)

	err := os.MkdirAll(fmt.Sprintf("%s/%s", strings.Join(filenameParts[:len(filenameParts)-2], "/"), parentDir), 0755)
	if err != nil {
		return err
	}

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

// AddFileToZip - adds files to a zip file and writes them out
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
	pieces := strings.Split(directoryName, "-")
	return pieces[0], pieces[1]
}
