//
// Created by Michał Łaszczewski on 27/07/16.
//

#ifndef WEAKFASTSET
#define WEAKFASTSET

#include <vector>
#include <memory>
#include <functional>
#include <boost/iterator/filter_iterator.hpp>

  template<typename ElementType> class WeakFastSet {
  public:
    std::vector<std::weak_ptr<ElementType>> buffer;

    WeakFastSet() {}
    ~WeakFastSet() {}

    int search = 0;

    int findEmpty() {
      int len = buffer.size();
      if(len) {
        for (int i = (search + 1) % len; i != search; i = (i + 1) % len) {
          if (buffer[i].expired()) { // Found empty
            search = i;
            return i;
          }
        }
      }
      search = len;
      buffer.resize(search+1024);
      return search;
    }

    int add(std::shared_ptr<ElementType> element) {
      //fox_log("BUFFER SIZE %d",(int)(buffer.size()));
      buffer[findEmpty()] = element;
      return search;
    }
    void remove(int id) {
      std::shared_ptr<ElementType> ptr = nullptr;
      buffer[id] = ptr;
    }
    void remove(std::shared_ptr<ElementType> element) {
      for(int i = 0; i < buffer.size(); i++) {
        if(buffer[i] == element) {
          buffer[i] = nullptr;
          return;
        }
      }
    }

    struct is_element {
      bool operator()(std::weak_ptr<ElementType> x) { return !x.expired(); }
    };

    auto begin() {
      return boost::make_filter_iterator<is_element>(buffer.begin(), buffer.end());
    }
    auto end() {
      return boost::make_filter_iterator<is_element>(buffer.end(), buffer.end());
    }

    std::shared_ptr<ElementType>& operator[](unsigned int id) {
      return buffer[id];
    }
    const std::shared_ptr<ElementType>& operator[](unsigned int id) const {
      return buffer[id];
    }

    unsigned int size() {
      return buffer.size();
    }
  };


#endif //FASTSET
