/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useState, useEffect, useRef } from 'react';

export interface Props {
  height?: number;
  isEmpty?: boolean;
  forceUpdate?: boolean;
}

const Comp: React.FC<Props> = () => {
  const [sourceNode, setSourceNode] = useState({ id: null, position: { x: 0, y: 0 } });
  const [temNode, setTemNode] = useState({ id: null, position: { x: 0, y: 0 } });
  const [flag, setFlag] = useState(false);

  const sourceElem = useRef(null);
  const targetElem = useRef(null);

  const Edge = ({ nodes: [sourceNode, targetNode] }) => (
    // <svg
    //   key={sourceNode.id + targetNode.id || ''}
    //   style={{
    //     position: 'absolute',
    //     overflow: 'visible',
    //     // zIndex: '-1',
    //     transform: 'translate(5px,5px)',
    //   }}
    // >
    //   <path
    //     d={`M ${sourceNode.position[0]} ${sourceNode.position[1]}
    //             C
    //             ${(targetNode.position[0] + sourceNode.position[0]) / 2} ${sourceNode.position[1]}
    //             ${(targetNode.position[0] + sourceNode.position[0]) / 2} ${targetNode.position[1]}
    //             ${targetNode.position[0]} ${targetNode.position[1]} `}
    //     strokeWidth={2}
    //     stroke={'red'}
    //     fill="none"
    //   ></path>
    // </svg>
    <svg
      key={sourceNode.id + targetNode.id || ''}
      style={{
        position: 'absolute',
        overflow: 'visible',
        // zIndex: '-1',
        transform: 'translate(5px,5px)',
      }}
    >
      {/* <path d={`M0 0,L900 0`} strokeWidth={2} stroke={'blue'} fill="none"></path> */}
      <path
        d={`M${sourceNode.position.x} ${sourceNode.position.y},L${targetNode.position.x} ${targetNode.position.y}`}
        strokeWidth={2}
        stroke={'blue'}
        fill="none"
      ></path>
    </svg>
  );

  useEffect(() => {
    // 开始拖拽
    // let bb = false;
    // sourceElem.current.addEventListener('dragstart', event => {
    //   event.dataTransfer.effectAllowed = 'move';
    //   console.log('naxie');
    //   setSourceNode({ id: 1, position: { x: 0, y: 0 } });
    //   bb = false;
    //   event.dataTransfer.setData('text/plain', 'true');
    //   const circles = document.getElementById('startId');
    //   // circles.style.cursor = 'move';
    // });
    // // 源元素拖拽持续触发
    // sourceElem.current.addEventListener('drag', event => {
    //   event.dataTransfer.effectAllowed = 'pointer';
    //   setTemNode({ id: 2, position: { x: event.offsetX, y: event.offsetY } });
    //   // if (flag === true) {
    //   //   setTemNode({ id: 2, position: { x: event.offsetX, y: event.offsetY } });
    //   // }
    // });
    // // 拖拽结束
    // sourceElem.current.addEventListener(
    //   'dragend',
    //   event => {
    //     if (bb === true) {
    //       setTemNode({ id: 2, position: { x: event.offsetX, y: event.offsetY } });
    //       return;
    //     }
    //     setTemNode({ id: 1, position: { x: 0, y: 0 } });
    //     bb = false;
    //   },
    //   true,
    // );
    // targetElem.current.addEventListener(
    //   'dragover',
    //   event => {
    //     bb = true;
    //     event.preventDefault();
    //     setTemNode({ id: 2, position: { x: event.offsetX, y: event.offsetY } });
    //   },
    //   true,
    // );
    // // 目标元素 拖拽释放持续触发
    // targetElem.current.addEventListener('drop', event => {
    //   bb = true;
    //   event.preventDefault();
    //   document.getElementById('endId').style.cursor = 'default';
    //   const test = event.dataTransfer.getData('text/plain');
    //   if (test === 'true') {
    //     setTemNode({ id: 2, position: { x: event.offsetX, y: event.offsetY } });
    //   }
    // });
  }, []);

  const [startPage, setStartPage] = useState({ x: null, y: null });

  // if (flag === true) {
  //   console.log('move flag ');
  //   document.onmousemove = function (ev) {
  //     var currentX = ev.offsetX; //鼠标移动中的当前位置  X
  //     var currentY = ev.offsetY; //鼠标移动中的当前位置  Y
  //     const circles = document.getElementById('startId');
  //     var clientRect = circles.getBoundingClientRect(); //图形的坐标
  //     const end = document.getElementById('endId');
  //     var clientRectEnd = end.getBoundingClientRect(); //图形的坐标
  //     var distanceX = Math.abs(currentX - clientRect.x);
  //     var distanceY = Math.abs(currentY - clientRect.y);
  //     var distanceXE = Math.abs(currentX - clientRectEnd.x);
  //     var distanceYE = Math.abs(currentY - clientRectEnd.y);
  //     // console.log(clientRect, 'clientRect');
  //     // console.log(currentX, currentY, 'ev');
  //     // setSourceNode({ id: 1, position: [-5, -5] });
  //     // console.log(clientRectEnd.x, distanceX, 'endXY');
  //     // setTemNode({ id: 2, position: [distanceXE, -5] });
  //     // setTemNode({ id: 2, position: { x: currentX - startPage.x, y: -5 } });
  //     // setTemNode({ id: 2, position: { x: currentX, y: -5 } });
  //     if (distanceXE < 5 && distanceYE < 5) {
  //       // setSourceNode({ id: 1, position: [-3, -5] });
  //       //距离圆心小于 5
  //       console.log('小于5');
  //       // circles.style.backgroundColor = 'blue';
  //       // circles.style.cursor = 'move';
  //       // setTemNode({ id: 2, position: [distanceX, -5] });
  //       // setTemNode({ id: 2, position: [clientRectEnd.x - distanceX, -5] });
  //       // setTemNode({ id: 2, position: { x: 903, y: -5 } });
  //       return;
  //     }
  //     setFlag(false);
  //   };
  // }

  document.onmousemove = function (ev) {
    var currentX = ev.offsetX; //鼠标移动中的当前位置  X
    var currentY = ev.offsetY; //鼠标移动中的当前位置  Y
    const circles = document.getElementById('startId');
    var clientRect = circles.getBoundingClientRect(); //图形的坐标
    var distanceX = Math.abs(currentX - clientRect.x);
    var distanceY = Math.abs(currentY - clientRect.y);
    // console.log(clientRect, 'clientRect');
    console.log(distanceX, 'x');
    console.log(distanceX, 'y');
    if (distanceX < 5 && distanceY < 5) {
      //距离圆心小于 5
      // console.log('小于5');
      circles.style.backgroundColor = 'blue';
      // circles.style.cursor = 'move';
    } else {
      setTemNode({ id: 2, position: { x: distanceX, y: distanceY } });
      // if (flag === true) {
      //   setTemNode({ id: 2, position: [distanceX, distanceY] });
      // }
      // setFlag(false);
      circles.style.backgroundColor = 'grey';
      // circles.style.cursor = 'crosshair';
    }
    if (distanceX > 900 && distanceY > 900 && flag === true) {
      setTemNode({ id: 2, position: { x: distanceX, y: distanceY } });
      return;
    }
  };

  // function mouseDownAndMove(dom, callback) {
  //   let flag = false;
  //   let i = true;
  //   let fn = function (e) {
  //     if (i) callback(e);
  //   };
  //   // 添加鼠标按下监听
  //   dom.addEventListener('mousedown', function (even) {
  //     // 当鼠标按下时, 添加鼠标移动监听
  //     flag = true;
  //     dom.addEventListener('mousemove', fn);
  //   });

  //   // 添加鼠标弹起监听 , 即鼠标不在按下
  //   dom.addEventListener('mouseup', function () {
  //     // 此时移除 鼠标移动监听,移除指定 事件函数
  //     i = false;
  //     dom.removeEventListener('mousemove', fn);
  //   });
  //   // 当鼠标在其他元素中弹起时的操作, 规避鼠标弹起后 dom 无法捕获的异常
  //   document.addEventListener('mouseup', function () {
  //     // 此时移除 鼠标移动监听,移除指定 事件函数
  //     flag = false;
  //     dom.removeEventListener('mousemove', fn);
  //   });
  // }

  const onClick = e => {
    // setSourceNode({ id: 1, position: [-3, -5] });
    console.log(e, 'start');
    setStartPage({ x: e.pageX, y: e.pageY });
    // setFlag(true);
  };

  const startDiv = e => {
    setFlag(true);
    setStartPage({ x: e.pageX, y: e.pageY });
    // console.log(e, 12);
    // setSourceNode({ id: 1, position: [-5, -10] });
  };
  const onMove = e => {
    // console.log(e, 'move');
  };

  const onUp = e => {
    // console.log(e, 'move');
    // setSourceNode({ id: 1, position: [-5, -5] });
    setFlag(false);
  };

  const endDiv = e => {
    setFlag(false);
    console.log(123);
    // setSourceNode({ id: 1, position: [0, 0] });
    // setTemNode({ id: 2, position: [e.pageX - startPage.x, 0] });
    // setTemNode({ id: 2, position: [903, 0] });
    // return <Edge nodes={[sourceNode, temNode]}></Edge>;
    // var currentX = e.nativeEvent.offsetX; //鼠标移动中的当前位置  X
    // var currentY = e.nativeEvent.offsetY; //鼠标移动中的当前位置  Y
    // var distanceX = Math.abs(currentX - e.nativeEvent.x);
    // var distanceY = Math.abs(currentY - e.nativeEvent.y);
    // // console.log(distanceX, distanceY);
    // setTemNode({ id: 2, position: [distanceX, distanceY] });
    setTemNode({ id: 2, position: { x: 903, y: 0 } });
    // setTemNode({ id: 2, position: { x: 903, y: 0 } });
  };

  const onMouseLeave = e => {
    setFlag(false);
    // setSourceNode({ id: 1, position: [0, 0] });
    // setTemNode({ id: 2, position: [e.pageX - startPage.x, 0] });
    // setTemNode({ id: 2, position: [903, 0] });
    // return <Edge nodes={[sourceNode, temNode]}></Edge>;
    // var currentX = e.nativeEvent.offsetX; //鼠标移动中的当前位置  X
    // var currentY = e.nativeEvent.offsetY; //鼠标移动中的当前位置  Y
    // var distanceX = Math.abs(currentX - e.nativeEvent.x);
    // var distanceY = Math.abs(currentY - e.nativeEvent.y);
    // // console.log(distanceX, distanceY);
    // setTemNode({ id: 2, position: [distanceX, distanceY] });
    // setTemNode({ id: 2, position: { x: e.pageX - startPage.x, y: 0 } });
  };

  function mousePos(e) {
    if (e.pageX) {
      //IE9及以上支持pageX属性 相对文档
      return { x: e.pageX, y: e.pageY };
    } else {
      return {
        x: e.clientX + document.body.scrollLeft - document.body.clientLeft,
        y: e.clientY + document.body.scrollTop - document.body.clientTop,
      };
    }
  }

  return (
    <>
      <div style={{ width: 400, height: 300, position: 'absolute' }}>
        <div
          ref={sourceElem}
          id="startId"
          // draggable={true}
          // onClick={e => onClick(e)}
          // onDragStart={e => startDiv(e)}
          onMouseDown={e => startDiv(e)}
          onMouseMove={e => onMove(e)}
          onMouseUp={e => onUp(e)}
          style={{
            width: 10,
            height: 10,
            borderRadius: 50,
            backgroundColor: 'grey',
            marginTop: 100,
            // marginLeft: 20,
          }}
        ></div>
        <Edge nodes={[sourceNode, temNode]}></Edge>
        <div
          ref={targetElem}
          id="endId"
          draggable={true}
          // onClick={e => endDiv(e)}
          onMouseEnter={e => endDiv(e)}
          onMouseLeave={e => onMouseLeave(e)}
          style={{
            width: 40,
            height: 40,
            borderRadius: 50,
            backgroundColor: 'green',
            border: '1px solid green',
            position: 'absolute',
            marginLeft: 900,
            zIndex: 9999,
          }}
        ></div>
        <canvas></canvas>
        {/* <div
          ref={targetElem}
          style={{
            width: 50,
            height: 50,
            marginLeft: 900,
            border: '1px solid red',
            // position: 'relative',
          }}
        >
          <div
            // ref={targetElem}
            id="endId"
            draggable={true}
            // onClick={e => endDiv(e)}
            onMouseEnter={e => endDiv(e)}
            onMouseLeave={e => onMouseLeave(e)}
            style={{
              width: 10,
              height: 10,
              borderRadius: 50,
              backgroundColor: 'green',
              border: '1px solid green',
              position: 'absolute',
              marginLeft: 900,
              zIndex: 9999,
            }}
          ></div>
        </div> */}
      </div>
    </>
  );
};

export default Comp;
