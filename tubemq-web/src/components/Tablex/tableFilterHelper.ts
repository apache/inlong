interface TableFilterHelperProp {
  key: string;
  targetArray: Array<any>;
  srcArray: Array<any>;
  filterList: Array<any>;
  updateFunction?: (p: Array<any>) => void;
}
const tableFilterHelper = (p: TableFilterHelperProp): any[] => {
  const { key, srcArray = [], filterList, updateFunction } = p;
  const res: any[] = [];

  if (key) {
    srcArray.forEach(it => {
      const tar = filterList.map(t => {
        return it[t];
      });
      let isFilterRight = false;
      tar.forEach(t => {
        if ((t + '').indexOf(key) > -1) isFilterRight = true;
      });
      if (isFilterRight) {
        res.push(it);
      }
    });
  }

  if (updateFunction) updateFunction(res);

  return res;
};

export default tableFilterHelper;
