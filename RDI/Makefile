CXX := g++
CXXFLAGS := -std=c++17 -O3 -march=native -pthread -DRANGE_SINGLE_ENABLE_OPEN_RANGE=1 -DRANGE_SINGLE_DEBUG_OPEN_RANGE=1

SRC_DIR := src
BIN := rdi

SRC := \
	$(SRC_DIR)/main.cpp \
	$(SRC_DIR)/graph.cpp \
	$(SRC_DIR)/NFA_state.cpp \
	$(SRC_DIR)/Parser.cpp \
	$(SRC_DIR)/Regex.cpp \
	$(SRC_DIR)/Traversal.cpp \
	$(SRC_DIR)/suffix_array.cpp \
	$(SRC_DIR)/ProductionFA.cpp \
	$(SRC_DIR)/NestedCountEvaluator.cpp \
	$(SRC_DIR)/Pattern.cpp \
	$(SRC_DIR)/AdditionChainTable.cpp

INCLUDES := -I$(SRC_DIR) -Ithird_party/croaring/include
LIBS := third_party/croaring/libcroaring.a

all: $(BIN)

$(BIN): $(SRC) $(wildcard $(SRC_DIR)/*.h) $(wildcard $(SRC_DIR)/vendor/*.h)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $(SRC) $(LIBS)

clean:
	rm -f $(BIN)
