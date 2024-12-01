package main

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"time"
)

type Produto struct {
	ID           [4]byte
	ProductID    [4]byte
	Price        [4]byte
	Brand        [20]byte
	CategoryCode [20]byte
}
type Acesso struct {
	ID          [4]byte
	UserSession [20]byte
	UserID      [4]byte
	EventType   [10]byte
}

func padString(str string, length int) []byte {
	padded := make([]byte, length)
	copy(padded, str)
	return padded
}
func int32ToBytes(n int32) [4]byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(n))
	return buf
}
func float32ToBytes(f float32) [4]byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], math.Float32bits(f))
	return buf
}

type HashTable struct {
	buckets [][]Acesso
	size    int
}

func hashUserSession(userSession [20]byte, tableSize int) int {
	var hash uint32
	for i := 0; i < len(userSession); i++ {
		hash = (hash * 16777619) ^ uint32(userSession[i])
	}
	return int(hash % uint32(tableSize))
}
func newHashTable(size int) *HashTable {
	return &HashTable{
		buckets: make([][]Acesso, size),
		size:    size,
	}
}
func (ht *HashTable) insert(acesso Acesso) {
	index := hashUserSession(acesso.UserSession, ht.size)
	ht.buckets[index] = append(ht.buckets[index], acesso)
}
func (ht *HashTable) search(userSession [20]byte) []Acesso {
	index := hashUserSession(userSession, ht.size)
	return ht.buckets[index]
}
func (ht *HashTable) rebuild(acessos []Acesso) {
	ht.buckets = make([][]Acesso, ht.size)
	for _, acesso := range acessos {
		ht.insert(acesso)
	}
}
func (ht *HashTable) remove(userSession [20]byte) {
	var updatedAcessos []Acesso
	for _, bucket := range ht.buckets {
		for _, acesso := range bucket {
			if acesso.UserSession != userSession {
				updatedAcessos = append(updatedAcessos, acesso)
			}
		}
	}
	ht.rebuild(updatedAcessos)
}
func processCSV(filePath string) ([]Produto, []Acesso, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("erro ao abrir o arquivo CSV: %w", err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("erro ao ler o cabeçalho: %w", err)
	}
	produtoIDCounter := int32(1)
	acessoIDCounter := int32(1)
	produtoArr := []Produto{}
	acessoArr := []Acesso{}
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, fmt.Errorf("erro ao ler o arquivo CSV: %w", err)
		}
		productID, _ := strconv.ParseInt(record[3], 10, 32)
		price, _ := strconv.ParseFloat(record[6], 32)
		userID, _ := strconv.ParseInt(record[7], 10, 32)
		userSession := record[8]
		eventType := record[1]
		brand := record[5]
		categoryCode := record[4]
		var produto Produto
		produto.ID = int32ToBytes(produtoIDCounter)
		produto.ProductID = int32ToBytes(int32(productID))
		produto.Price = float32ToBytes(float32(price))
		copy(produto.Brand[:], padString(brand, 20))
		copy(produto.CategoryCode[:], padString(categoryCode, 20))
		var acesso Acesso
		acesso.ID = int32ToBytes(acessoIDCounter)
		copy(acesso.UserSession[:], padString(userSession, 20))
		acesso.UserID = int32ToBytes(int32(userID))
		copy(acesso.EventType[:], padString(eventType, 10))
		produtoIDCounter++
		acessoIDCounter++
		produtoArr = append(produtoArr, produto)
		acessoArr = append(acessoArr, acesso)
	}
	return produtoArr, acessoArr, nil
}
func createFileIndex(acessos []Acesso, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("erro ao criar o arquivo: %w", err)
	}
	defer file.Close()
	for _, acesso := range acessos {
		err := binary.Write(file, binary.LittleEndian, acesso)
		if err != nil {
			return fmt.Errorf("erro ao gravar no arquivo: %w", err)
		}
	}
	return nil
}
func searchFileIndex(filePath string, userSession [20]byte) ([]Acesso, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir o arquivo: %w", err)
	}
	defer file.Close()
	var acessos []Acesso
	for {
		var acesso Acesso
		err := binary.Read(file, binary.LittleEndian, &acesso)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("erro ao ler o arquivo: %w", err)
		}
		if acesso.UserSession == userSession {
			acessos = append(acessos, acesso)
		}
	}
	return acessos, nil
}
func removeFileIndex(filePath string, userSession [20]byte) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo: %w", err)
	}
	defer file.Close()
	var acessos []Acesso
	for {
		var acesso Acesso
		err := binary.Read(file, binary.LittleEndian, &acesso)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("erro ao ler o arquivo: %w", err)
		}
		acessos = append(acessos, acesso)
	}
	var updatedAcessos []Acesso
	for _, acesso := range acessos {
		if acesso.UserSession != userSession {
			updatedAcessos = append(updatedAcessos, acesso)
		}
	}
	file, err = os.Create(filePath)
	if err != nil {
		return fmt.Errorf("erro ao criar o arquivo: %w", err)
	}
	defer file.Close()
	for _, acesso := range updatedAcessos {
		err := binary.Write(file, binary.LittleEndian, acesso)
		if err != nil {
			return fmt.Errorf("erro ao gravar no arquivo: %w", err)
		}
	}
	return nil
}

type BTreeIndex struct {
	tree *BTree
}
type BTreeNode struct {
	keys   [][4]byte
	values []int64
	childs []*BTreeNode
}
type BTree struct {
	root  *BTreeNode
	order int
}

func NewBTreeIndex(pageSize int) *BTreeIndex {
	order := pageSize / (4 + 8)
	return &BTreeIndex{
		tree: &BTree{
			root:  &BTreeNode{},
			order: order,
		},
	}
}
func (idx *BTreeIndex) Insert(produto Produto, endereco int64) {
	id := produto.ID
	idx.tree.insert(id, endereco)
}
func (t *BTree) insert(key [4]byte, value int64) {
	if t.root == nil {
		t.root = &BTreeNode{}
	}
	t.insertRecursive(t.root, key, value)
}
func (t *BTree) insertRecursive(node *BTreeNode, key [4]byte, value int64) {
	if len(node.keys) < t.order-1 {
		node.keys = append(node.keys, key)
		node.values = append(node.values, value)
		return
	}
}
func (idx *BTreeIndex) Search(id [4]byte) (int64, bool) {
	return idx.tree.search(id)
}
func (t *BTree) search(key [4]byte) (int64, bool) {
	for i, k := range t.root.keys {
		if bytes.Equal(k[:], key[:]) {
			return t.root.values[i], true
		}
	}
	return 0, false
}
func (idx *BTreeIndex) Remove(id [4]byte) bool {
	return idx.tree.remove(id)
}
func (t *BTree) remove(key [4]byte) bool {
	return t.removeRecursive(t.root, key)
}
func (t *BTree) removeRecursive(node *BTreeNode, key [4]byte) bool {
	for i, k := range node.keys {
		if bytes.Equal(k[:], key[:]) {
			node.keys = append(node.keys[:i], node.keys[i+1:]...)
			node.values = append(node.values[:i], node.values[i+1:]...)
			return true
		}
	}
	for _, child := range node.childs {
		if child != nil {
			if t.removeRecursive(child, key) {
				return true
			}
		}
	}
	return false
}
func (idx *BTreeIndex) Rebuild(produtos []Produto) {
	idx.tree.root = &BTreeNode{}
	for _, produto := range produtos {
		idx.Insert(produto, int64(len(produtos)))
	}
}

type ProdutoFileIndex struct {
	fileName string
}

func NewProdutoFileIndex(fileName string) *ProdutoFileIndex {
	return &ProdutoFileIndex{
		fileName: fileName,
	}
}
func (idx *ProdutoFileIndex) openFile() (*os.File, error) {
	file, err := os.OpenFile(idx.fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}
func (idx *ProdutoFileIndex) Insert(produto Produto) error {
	file, err := idx.openFile()
	if err != nil {
		return err
	}
	defer file.Close()
	var produtos []Produto
	if _, err := file.Seek(0, 0); err == nil {
		produtos, err = idx.readAll(file)
		if err != nil {
			return err
		}
	}
	produtos = append(produtos, produto)
	sort.Slice(produtos, func(i, j int) bool {
		return bytes.Compare(produtos[i].ID[:], produtos[j].ID[:]) < 0
	})
	file.Truncate(0)
	file.Seek(0, 0)
	for _, p := range produtos {
		file.Write(produtoToBytes(p))
	}
	return nil
}
func (idx *ProdutoFileIndex) Search(id [4]byte) (Produto, bool, error) {
	file, err := idx.openFile()
	if err != nil {
		return Produto{}, false, err
	}
	defer file.Close()
	produtos, err := idx.readAll(file)
	if err != nil {
		return Produto{}, false, err
	}
	for _, p := range produtos {
		if bytes.Equal(p.ID[:], id[:]) {
			return p, true, nil
		}
	}
	return Produto{}, false, nil
}
func (idx *ProdutoFileIndex) Remove(id [4]byte) error {
	file, err := idx.openFile()
	if err != nil {
		return err
	}
	defer file.Close()
	produtos, err := idx.readAll(file)
	if err != nil {
		return err
	}
	var produtosRestantes []Produto
	for _, p := range produtos {
		if !bytes.Equal(p.ID[:], id[:]) {
			produtosRestantes = append(produtosRestantes, p)
		}
	}
	if len(produtosRestantes) == len(produtos) {
		return fmt.Errorf("produto com ID %v não encontrado", id)
	}
	file.Truncate(0)
	file.Seek(0, 0)
	for _, p := range produtosRestantes {
		file.Write(produtoToBytes(p))
	}
	return nil
}
func (idx *ProdutoFileIndex) Rebuild(produtos []Produto) error {
	file, err := idx.openFile()
	if err != nil {
		return err
	}
	defer file.Close()
	sort.Slice(produtos, func(i, j int) bool {
		return bytes.Compare(produtos[i].ID[:], produtos[j].ID[:]) < 0
	})
	file.Truncate(0)
	file.Seek(0, 0)
	for _, p := range produtos {
		file.Write(produtoToBytes(p))
	}
	return nil
}
func (idx *ProdutoFileIndex) readAll(file *os.File) ([]Produto, error) {
	var produtos []Produto
	var produto Produto
	_, err := file.Seek(0, 0)
	if err != nil {
		return nil, err
	}
	for {
		err := binary.Read(file, binary.LittleEndian, &produto)
		if err != nil {
			break
		}
		produtos = append(produtos, produto)
	}
	return produtos, nil
}
func produtoToBytes(produto Produto) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, produto)
	return buf.Bytes()
}
func main() {
	produtos, acessos, err := processCSV("t.csv")
	if err != nil {
		fmt.Println("Erro:", err)
		return
	}
	start := time.Now()
	hashTable := newHashTable(1000)
	for _, acesso := range acessos {
		hashTable.insert(acesso)
	}
	memoryIndexCreationDuration := time.Since(start)
	start = time.Now()
	hashTable.search(acessos[0].UserSession)
	hashTable.search(acessos[10].UserSession)
	hashTable.search(acessos[50].UserSession)
	hashTable.search(acessos[100].UserSession)
	hashTable.search(acessos[998].UserSession)
	memorySearchDuration := time.Since(start)
	start = time.Now()
	hashTable.remove(acessos[0].UserSession)
	hashTable.remove(acessos[10].UserSession)
	hashTable.remove(acessos[50].UserSession)
	hashTable.remove(acessos[100].UserSession)
	hashTable.remove(acessos[998].UserSession)
	memoryRemoveDuration := time.Since(start)
	start = time.Now()
	err = createFileIndex(acessos, "index.bin")
	if err != nil {
		fmt.Println("Erro ao criar o índice:", err)
		return
	}
	fileIndexCreationDuration := time.Since(start)
	start = time.Now()
	_, err = searchFileIndex("index.bin", acessos[0].UserSession)
	_, err = searchFileIndex("index.bin", acessos[10].UserSession)
	_, err = searchFileIndex("index.bin", acessos[50].UserSession)
	_, err = searchFileIndex("index.bin", acessos[100].UserSession)
	_, err = searchFileIndex("index.bin", acessos[998].UserSession)
	if err != nil {
		fmt.Println("Erro ao pesquisar no arquivo:", err)
		return
	}
	fileSearchDuration := time.Since(start)
	start = time.Now()
	err = removeFileIndex("index.bin", acessos[0].UserSession)
	err = removeFileIndex("index.bin", acessos[10].UserSession)
	err = removeFileIndex("index.bin", acessos[50].UserSession)
	err = removeFileIndex("index.bin", acessos[100].UserSession)
	err = removeFileIndex("index.bin", acessos[998].UserSession)
	if err != nil {
		fmt.Println("Erro ao remover do índice:", err)
		return
	}
	fileRemoveDuration := time.Since(start)
	fmt.Printf("Hash - Tempo de criação do índice em memória: %v\n", memoryIndexCreationDuration)
	fmt.Printf("Hash - Tempo de pesquisa no índice em memória: %v\n", memorySearchDuration)
	fmt.Printf("Hash - Tempo de criação do índice em arquivo: %v\n", fileIndexCreationDuration)
	fmt.Printf("Hash - Tempo de pesquisa no índice em arquivo: %v\n", fileSearchDuration)
	fmt.Printf("Hash - Tempo de remoção e recriação no índice em memória: %v\n", memoryRemoveDuration)
	fmt.Printf("Hash - Tempo de remoção e recriação no índice em arquivo: %v\n", fileRemoveDuration)
	start = time.Now()
	tree := NewBTreeIndex(256)
	for i, produto := range produtos {
		tree.Insert(produto, int64(i))
	}
	memoryIndexCreationDuration = time.Since(start)
	start = time.Now()
	tree.Search([4]byte{0, 0, 0, 0})
	tree.Search([4]byte{10, 0, 0, 0})
	tree.Search([4]byte{50, 0, 0, 0})
	tree.Search([4]byte{100, 0, 0, 0})
	tree.Search([4]byte{230, 3, 0, 0})
	memorySearchDuration = time.Since(start)
	start = time.Now()
	tree.Remove([4]byte{0, 0, 0, 0})
	tree.Remove([4]byte{10, 0, 0, 0})
	tree.Remove([4]byte{50, 0, 0, 0})
	tree.Remove([4]byte{100, 0, 0, 0})
	tree.Remove([4]byte{230, 3, 0, 0})
	memoryRemoveDuration = time.Since(start)
	start = time.Now()
	index := NewProdutoFileIndex("produto_index.bin")
	for _, produto := range produtos {
		err := index.Insert(produto)
		if err != nil {
			fmt.Println("Erro ao inserir produto:", err)
		}
	}
	fileIndexCreationDuration = time.Since(start)
	start = time.Now()
	index.Search([4]byte{0, 0, 0, 0})
	index.Search([4]byte{10, 0, 0, 0})
	index.Search([4]byte{50, 0, 0, 0})
	index.Search([4]byte{100, 0, 0, 0})
	index.Search([4]byte{230, 3, 0, 0})
	fileSearchDuration = time.Since(start)
	start = time.Now()
	index.Remove([4]byte{0, 0, 0, 0})
	index.Search([4]byte{10, 0, 0, 0})
	index.Search([4]byte{50, 0, 0, 0})
	index.Search([4]byte{100, 0, 0, 0})
	index.Search([4]byte{230, 3, 0, 0})
	fileRemoveDuration = time.Since(start)
	fmt.Printf("B+ Tree - Tempo de criação do índice em memória: %v\n", memoryIndexCreationDuration)
	fmt.Printf("B+ Tree - Tempo de pesquisa no índice em memória: %v\n", memorySearchDuration)
	fmt.Printf("B+ Tree - Tempo de criação do índice em arquivo: %v\n", fileIndexCreationDuration)
	fmt.Printf("B+ Tree - Tempo de pesquisa no índice em arquivo: %v\n", fileSearchDuration)
	fmt.Printf("B+ Tree - Tempo de remoção e recriação no índice em memória: %v\n", memoryRemoveDuration)
	fmt.Printf("B+ Tree - Tempo de remoção e recriação no índice em arquivo: %v\n", fileRemoveDuration)
}
